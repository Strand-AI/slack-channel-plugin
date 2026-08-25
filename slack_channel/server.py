"""Slack channel plugin — outbound MCP server over stdio.

Exposes reply + read tools so an agent can post to Slack, read history and react.
It does not listen: there is no Socket Mode connection, no event bus and no
cold-reply path.

The passive listener was removed deliberately. It spawned a `claude -p` for every
unowned message in a watched channel, which was expensive and could answer people
who were not talking to the agent. Inbound Slack is handled outside this plugin.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import re
import sys
import time
import urllib.request
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anyio
import mcp.types as types
from dotenv import load_dotenv
from mcp.server.lowlevel.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.session import ServerSession
from mcp.server.stdio import stdio_server

# Load env vars. Priority: real env vars > plugin channels dir > local .env
# The channels dir (~/.claude/channels/slack-channel/.env) is the standard
# location for plugin credentials, written by /slack-channel:configure.
_channels_env = Path.home() / ".claude" / "channels" / "slack-channel" / ".env"
_pkg_dir = Path(__file__).resolve().parent.parent
load_dotenv(_channels_env)
load_dotenv(_pkg_dir / ".env")
load_dotenv(_pkg_dir.parent / ".env")

logger = logging.getLogger("slack-channel")

# ---------------------------------------------------------------------------
# Globals (set during startup)
# ---------------------------------------------------------------------------
_session: ServerSession | None = None
_slack_client: Any = None  # AsyncWebClient (bot token) — used for writes + notifications
_read_client: Any = None   # AsyncWebClient (user xoxp token if set, else falls back to bot) — used for reads
_search_client: Any = None  # AsyncWebClient (user xoxp token only) — workspace-wide search
_bot_user_id: str | None = None
_user_name_cache: dict[str, str] = {}

CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "")
_SLACK_CHANNEL_ID_RE = re.compile(r"^[CDG][A-Z0-9]+$")

# Shared state directory
_STATE_DIR = Path(
    os.environ.get("SLACK_CHANNEL_STATE_DIR", "")
) if os.environ.get("SLACK_CHANNEL_STATE_DIR") else (
    Path.home() / ".config" / "slack-channel"
)

# Thread tracking — persisted per conversation ID.
# The conversation ID is stable across --resume (it's what you pass to --resume).
# Resolved via a SessionStart hook that writes it to:
#   ~/.config/slack-channel/sessions/{claude-code-pid}.json
# The MCP server reads it using os.getppid() (its parent = Claude Code).
THREAD_TTL_DAYS = 7
_THREADS_FILE = _STATE_DIR / "threads.json"
_SESSIONS_DIR = _STATE_DIR / "sessions"
_conversation_id: str | None = None
_owned_threads: dict[str, str] = {}  # thread_ts → channel


def _get_ppid(pid: int) -> int:
    """Get the parent PID of a process (macOS/Linux)."""
    try:
        import subprocess
        result = subprocess.run(
            ["/bin/ps", "-o", "ppid=", "-p", str(pid)],
            capture_output=True, text=True, timeout=2,
        )
        return int(result.stdout.strip())
    except Exception:
        return 0


def _resolve_conversation_id() -> str | None:
    """Read the conversation ID written by the SessionStart hook.

    The hook writes ~/.config/slack-channel/sessions/{claude-code-pid}.json.
    We walk up the process tree (ppid → grandppid → ...) to find the matching
    session file, since there may be intermediate processes (e.g. uv/uvx)
    between Claude Code and us.
    """
    pid = os.getpid()
    for _ in range(5):  # walk up at most 5 levels
        pid = _get_ppid(pid)
        if pid <= 1:
            break
        session_file = _SESSIONS_DIR / f"{pid}.json"
        try:
            data = json.loads(session_file.read_text())
            return data.get("conversation_id")
        except (OSError, json.JSONDecodeError):
            continue
    return None


def _get_tmux_window() -> str | None:
    """Get the current tmux window name by querying tmux live.

    Uses $TMUX_PANE (inherited from the shell that spawned Claude Code)
    to identify the correct pane, then asks tmux for its window name.
    Resolved fresh each call so renames are picked up immediately.
    """
    pane = os.environ.get("TMUX_PANE")
    if not pane:
        return None
    try:
        import subprocess
        result = subprocess.run(
            ["tmux", "display-message", "-p", "-t", pane, "#{window_name}"],
            capture_output=True, text=True, timeout=2,
        )
        name = result.stdout.strip()
        return name or None
    except Exception:
        return None


def _ensure_conversation_id() -> str | None:
    """Resolve and cache the conversation ID. Retries until found."""
    global _conversation_id
    if _conversation_id is None:
        _conversation_id = _resolve_conversation_id()
        if _conversation_id:
            logger.info("Conversation ID: %s", _conversation_id)
            # Load persisted threads
            loaded = _load_threads()
            _owned_threads.update(loaded)
            if loaded:
                logger.info("Loaded %d threads for conversation %s", len(loaded), _conversation_id)
    return _conversation_id


def _load_threads() -> dict[str, str]:
    """Load threads for the current conversation from the shared threads file."""
    conv_id = _ensure_conversation_id()
    if not conv_id or not _THREADS_FILE.exists():
        return {}
    try:
        with open(_THREADS_FILE) as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    cutoff = time.time() - THREAD_TTL_DAYS * 86400
    return {
        t["thread_ts"]: t["channel"]
        for t in data
        if t.get("conversation_id") == conv_id
        and float(t["thread_ts"].split(".")[0]) > cutoff
    }


def _save_threads() -> None:
    """Persist this conversation's threads to the shared file."""
    conv_id = _ensure_conversation_id()
    if not conv_id:
        return
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    cutoff = time.time() - THREAD_TTL_DAYS * 86400

    with open(_THREADS_FILE, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        try:
            all_entries = json.load(f)
        except (json.JSONDecodeError, ValueError):
            all_entries = []
        # Keep other conversations' non-stale entries
        other = [
            t for t in all_entries
            if t.get("conversation_id") != conv_id
            and float(t["thread_ts"].split(".")[0]) > cutoff
        ]
        # Add this conversation's entries
        mine = [
            {"thread_ts": ts, "channel": ch, "conversation_id": conv_id}
            for ts, ch in _owned_threads.items()
        ]
        f.seek(0)
        f.truncate()
        json.dump(other + mine, f, indent=2)
        f.write("\n")




# ---------------------------------------------------------------------------
# Channel/DM name resolution
# ---------------------------------------------------------------------------

def _clean_channel_ref(channel: str) -> str:
    value = channel.strip()
    channel_link = re.fullmatch(r"<#([A-Z0-9]+)(?:\|[^>]+)?>", value)
    if channel_link:
        return channel_link.group(1)
    user_mention = re.fullmatch(r"<@([A-Z0-9]+)(?:\|[^>]+)?>", value)
    if user_mention:
        return user_mention.group(1)
    return value


def _normalize_lookup(value: str) -> str:
    value = value.strip().lower().lstrip("#@")
    return re.sub(r"[^a-z0-9]+", "", value)


def _is_channel_id(channel: str) -> bool:
    return bool(_SLACK_CHANNEL_ID_RE.fullmatch(channel))


async def _list_conversations_for_resolution(limit: int = 1000) -> list[dict]:
    conversations: list[dict] = []
    cursor: str | None = None
    while len(conversations) < limit:
        kwargs: dict[str, Any] = {
            "types": "public_channel,private_channel,mpim,im",
            "exclude_archived": True,
            "limit": min(200, limit - len(conversations)),
        }
        if cursor:
            kwargs["cursor"] = cursor
        result = await _read_client.conversations_list(**kwargs)
        conversations.extend(result.get("channels", []))
        cursor = result.get("response_metadata", {}).get("next_cursor") or None
        if not cursor:
            break
    return conversations


async def _conversation_label(ch: dict) -> tuple[str, str]:
    if ch.get("is_im"):
        peer = await _resolve_user(ch.get("user", ""))
        return f"@{peer}", "DM"
    name = ch.get("name") or ch.get("id", "?")
    if ch.get("is_mpim"):
        return f"#{name}", "group DM"
    return f"#{name}", f"{ch.get('num_members', '?')} members"


async def _conversation_terms(ch: dict) -> tuple[str, list[str]]:
    label, _ = await _conversation_label(ch)
    terms = [ch.get("id", ""), label, label.lstrip("#@")]
    if ch.get("name"):
        terms.extend([ch["name"], f"#{ch['name']}"])
    if ch.get("is_im") and ch.get("user"):
        user_id = ch["user"]
        terms.append(user_id)
        terms.append(f"@{user_id}")
    return label, terms


async def _resolve_channel_ref(channel: str) -> str:
    """Resolve a human Slack channel reference to a conversation ID.

    Accepts Slack conversation IDs plus human forms such as "#general",
    "general", "@alice", "Alice", or an exact group-DM label. A single
    distinctive substring is allowed so requests like "check my DMs with alice"
    can succeed without forcing the agent to copy opaque IDs.
    """
    query = _clean_channel_ref(channel)
    if not query:
        raise ValueError("channel is required")
    if _is_channel_id(query):
        return query

    query_norm = _normalize_lookup(query)
    if not query_norm:
        raise ValueError(f"Channel not found: {channel!r}")

    exact_matches: list[tuple[str, str]] = []
    fuzzy_matches: list[tuple[str, str]] = []
    # Track which matched conversations are 1:1 DMs so a bare person name can
    # prefer a direct conversation over a group DM whose name merely contains it.
    is_im_by_cid: dict[str, bool] = {}

    for ch in await _list_conversations_for_resolution():
        cid = ch.get("id", "")
        label, terms = await _conversation_terms(ch)
        normalized_terms = {_normalize_lookup(term) for term in terms if term}
        if query_norm in normalized_terms:
            exact_matches.append((cid, label))
        elif any(query_norm in term for term in normalized_terms):
            fuzzy_matches.append((cid, label))
        is_im_by_cid[cid] = bool(ch.get("is_im"))

    matches = exact_matches or fuzzy_matches
    if not matches:
        raise ValueError(
            f"Channel or DM not found for {channel!r}. Try list_channels, then pass "
            "the shown #channel/@user label."
        )

    # Deduplicate aliases that pointed at the same conversation.
    deduped: dict[str, str] = {}
    for cid, label in matches:
        deduped.setdefault(cid, label)
    matches = list(deduped.items())

    # When a query hits several conversations but exactly one is a 1:1 DM,
    # prefer it: typing a person's name almost always means their DM, not a
    # group DM that happens to include them.
    if len(matches) > 1:
        im_matches = [(cid, label) for cid, label in matches if is_im_by_cid.get(cid)]
        if len(im_matches) == 1:
            return im_matches[0][0]

    if len(matches) == 1:
        return matches[0][0]

    labels = ", ".join(f"{label} ({cid})" for cid, label in matches[:8])
    extra = "" if len(matches) <= 8 else f", and {len(matches) - 8} more"
    raise ValueError(
        f"Ambiguous channel or DM {channel!r}. Retry with one of: {labels}{extra}."
    )


# ---------------------------------------------------------------------------
# MCP server (low-level for full control over the run loop)
# ---------------------------------------------------------------------------
server = Server("slack-channel")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="reply",
            description="Send a message to a Slack channel, DM, group DM, or thread",
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Message text (supports Slack mrkdwn)"},
                    "channel": {
                        "type": "string",
                        "description": (
                            "Channel/DM name or ID. Prefer human names: #general, general, "
                            "@alice, Alice, or an exact group-DM label. "
                            "Defaults to SLACK_CHANNEL_ID env var."
                        ),
                    },
                    "thread_ts": {
                        "type": "string",
                        "description": "Thread timestamp to reply in an existing thread",
                    },
                },
                "required": ["text"],
            },
        ),
        types.Tool(
            name="add_reaction",
            description="Add an emoji reaction to a message",
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "description": "Channel/DM name or ID. Prefer names like #general or @alice.",
                    },
                    "timestamp": {"type": "string", "description": "Message timestamp"},
                    "reaction": {
                        "type": "string",
                        "description": "Emoji name without colons (e.g. thumbsup, eyes)",
                    },
                },
                "required": ["channel", "timestamp", "reaction"],
            },
        ),
        types.Tool(
            name="remove_reaction",
            description="Remove an emoji reaction from a message",
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "description": "Channel/DM name or ID. Prefer names like #general or @alice.",
                    },
                    "timestamp": {"type": "string", "description": "Message timestamp"},
                    "reaction": {
                        "type": "string",
                        "description": "Emoji name without colons (e.g. eyes)",
                    },
                },
                "required": ["channel", "timestamp", "reaction"],
            },
        ),
        types.Tool(
            name="list_channels",
            description=(
                "List Slack channels and DMs. Use the shown #channel/@user label, "
                "or a distinctive person/name substring, with read_history/reply/get_thread. "
                "Slack IDs are included only for debugging."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max channels (default 100)"},
                },
            },
        ),
        types.Tool(
            name="read_history",
            description=(
                "Read recent messages from a Slack channel, DM, or group DM. "
                "The channel field accepts names such as #general, general, @alice, Alice, "
                "exact group-DM labels, or Slack IDs. Prefer names."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "description": "Channel/DM name or ID. Prefer names like #general or @alice.",
                    },
                    "limit": {"type": "integer", "description": "Max messages (default 25)"},
                },
                "required": ["channel"],
            },
        ),
        types.Tool(
            name="search_messages",
            description=(
                "Search message history across the Slack workspace using Slack search syntax. "
                "Examples: 'roadmap', 'from:@alice in:project', "
                "'after:2026-01-01 before:2026-02-01 has:link'. Returns channel labels, "
                "timestamps, permalinks, and bounded thread context. This calls Slack's search "
                "API rather than scanning recent history and requires SLACK_USER_TOKEN with "
                "the search:read user scope."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Slack search query, including any native search modifiers.",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "description": "Results per page (default 20, max 100).",
                    },
                    "page": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "description": "Result page, 1-indexed (default 1, max 100).",
                    },
                    "sort": {
                        "type": "string",
                        "enum": ["score", "timestamp"],
                        "description": "Sort matches by relevance score (default) or timestamp.",
                    },
                    "sort_dir": {
                        "type": "string",
                        "enum": ["asc", "desc"],
                        "description": "Sort direction (default desc).",
                    },
                    "include_thread_context": {
                        "type": "boolean",
                        "description": (
                            "Include bounded context for up to five matched threads (default true)."
                        ),
                    },
                    "thread_context_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 20,
                        "description": "Messages shown per included thread (default 6, max 20).",
                    },
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="get_thread",
            description="Get all replies in a Slack thread",
            inputSchema={
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "description": "Channel/DM name or ID. Prefer names like #general or @alice.",
                    },
                    "thread_ts": {"type": "string", "description": "Thread timestamp"},
                },
                "required": ["channel", "thread_ts"],
            },
        ),
        types.Tool(
            name="fetch_file",
            description=(
                "Download a file attached to a Slack message and return a local path to Read. "
                "Works for any attachment — images, PDFs, CSVs, etc. Pass the file id shown in "
                "message annotations as '[... · fetch_file id=Fxxxx]'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_id": {
                        "type": "string",
                        "description": "Slack file id (e.g. F123ABC), from a message's fetch_file annotation.",
                    },
                },
                "required": ["file_id"],
            },
        ),
        types.Tool(
            name="debug",
            description="Show internal plugin state (owned threads, leader status, etc.)",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(
    name: str, arguments: dict[str, Any] | None
) -> list[types.TextContent]:
    args = arguments or {}
    handlers = {
        "reply": _handle_reply,
        "add_reaction": _handle_add_reaction,
        "remove_reaction": _handle_remove_reaction,
        "list_channels": _handle_list_channels,
        "read_history": _handle_read_history,
        "search_messages": _handle_search_messages,
        "get_thread": _handle_get_thread,
        "fetch_file": _handle_fetch_file,
        "debug": _handle_debug,
    }
    handler = handlers.get(name)
    if not handler:
        raise ValueError(f"Unknown tool: {name}")
    return await handler(args)


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

async def _handle_reply(args: dict) -> list[types.TextContent]:
    channel_arg = args.get("channel") or CHANNEL_ID
    if not channel_arg:
        return [types.TextContent(type="text", text="Error: no channel — set SLACK_CHANNEL_ID or pass channel")]
    try:
        channel = await _resolve_channel_ref(channel_arg)
    except ValueError as e:
        return [types.TextContent(type="text", text=f"Error: {e}")]

    # Prefix message with tmux window name if available (so recipients
    # know which agent/session sent the message).
    text = args["text"]
    tmux_window = _get_tmux_window()
    if tmux_window:
        text = f"[{tmux_window}] {text}"

    thread_ts = args.get("thread_ts")
    result = await _slack_client.chat_postMessage(
        channel=channel,
        text=text,
        thread_ts=thread_ts,
    )
    ts = result.get("ts", "?")

    # Track thread for routing (persisted per conversation ID).
    owned_ts = thread_ts or ts
    if owned_ts not in _owned_threads:
        _owned_threads[owned_ts] = channel
        _save_threads()
        logger.info("Tracking thread %s in %s", owned_ts, channel)

    return [types.TextContent(type="text", text=f"Sent (ts={ts}, channel={channel})")]


async def _handle_add_reaction(args: dict) -> list[types.TextContent]:
    try:
        channel = await _resolve_channel_ref(args["channel"])
    except ValueError as e:
        return [types.TextContent(type="text", text=f"Error: {e}")]
    await _slack_client.reactions_add(
        channel=channel,
        timestamp=args["timestamp"],
        name=args["reaction"],
    )
    return [types.TextContent(type="text", text=f"Reacted :{args['reaction']}:")]


async def _handle_remove_reaction(args: dict) -> list[types.TextContent]:
    try:
        channel = await _resolve_channel_ref(args["channel"])
    except ValueError as e:
        return [types.TextContent(type="text", text=f"Error: {e}")]
    try:
        await _slack_client.reactions_remove(
            channel=channel,
            timestamp=args["timestamp"],
            name=args["reaction"],
        )
    except Exception:
        pass  # Ignore if reaction wasn't there
    return [types.TextContent(type="text", text=f"Removed :{args['reaction']}:")]


async def _handle_list_channels(args: dict) -> list[types.TextContent]:
    limit = args.get("limit", 100)
    lines = []
    for ch in await _list_conversations_for_resolution(limit=limit):
        cid = ch["id"]
        label, detail = await _conversation_label(ch)
        lines.append(f'{label}  id={cid}  ({detail})  use channel="{label}"')
    return [types.TextContent(type="text", text="\n".join(lines) or "No channels found.")]


async def _handle_read_history(args: dict) -> list[types.TextContent]:
    try:
        channel = await _resolve_channel_ref(args["channel"])
    except ValueError as e:
        return [types.TextContent(type="text", text=f"Error: {e}")]
    limit = args.get("limit", 25)
    result = await _read_client.conversations_history(channel=channel, limit=limit)

    lines = []
    for msg in reversed(result.get("messages", [])):
        user = await _resolve_user(msg.get("user", ""))
        ts = msg.get("ts", "")
        text = msg.get("text", "") + _file_annotations(msg)
        thread_indicator = ""
        if msg.get("reply_count"):
            thread_indicator = f" [{msg['reply_count']} replies]"
        lines.append(f"[{ts}] {user}: {text}{thread_indicator}")
    return [types.TextContent(type="text", text="\n".join(lines) or "No messages.")]


_SEARCH_HIGHLIGHT_START = "\ue000"
_SEARCH_HIGHLIGHT_END = "\ue001"
_MAX_CONTEXT_THREADS = 5
_MAX_THREAD_CONTEXT_MESSAGES = 500


def _bounded_integer_arg(
    args: dict, name: str, default: int, minimum: int, maximum: int,
) -> tuple[int | None, str | None]:
    value = args.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        return None, f"{name} must be an integer from {minimum} to {maximum}"
    return value, None


def _format_slack_timestamp(ts: str) -> str:
    try:
        value = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        return value.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError, OSError):
        return "unknown time"


def _clean_search_text(text: str) -> str:
    return text.replace(_SEARCH_HIGHLIGHT_START, "").replace(_SEARCH_HIGHLIGHT_END, "")


async def _search_channel_label(match: dict) -> tuple[str, str]:
    channel = match.get("channel") or {}
    channel_id = channel.get("id", "") or "?"
    name = channel.get("name", "") or ""
    if match.get("type") == "im" or channel_id.startswith("D"):
        if name.startswith("U"):
            return f"@{await _resolve_user(name)}", channel_id
        return f"@{name or channel_id}", channel_id
    if channel.get("is_mpim"):
        return f"#{name or channel_id}", channel_id
    return f"#{name or channel_id}", channel_id


async def _fetch_thread_messages(channel_id: str, root_ts: str) -> list[dict] | None:
    messages: list[dict] = []
    cursor: str | None = None
    try:
        while len(messages) < _MAX_THREAD_CONTEXT_MESSAGES:
            kwargs: dict[str, Any] = {
                "channel": channel_id,
                "ts": root_ts,
                "limit": min(100, _MAX_THREAD_CONTEXT_MESSAGES - len(messages)),
            }
            if cursor:
                kwargs["cursor"] = cursor
            response = await _read_client.conversations_replies(**kwargs)
            messages.extend(response.get("messages", []))
            cursor = response.get("response_metadata", {}).get("next_cursor") or None
            if not cursor:
                break
        return messages
    except Exception as e:
        logger.warning("Could not fetch context for Slack thread %s/%s: %s", channel_id, root_ts, e)
        return None


async def _format_thread_context(
    messages: list[dict], match_ts: str, display_limit: int,
) -> list[str]:
    """Return a small window from a matched thread, centered on the match when possible."""
    if not messages:
        return []
    match_index = next(
        (index for index, message in enumerate(messages) if message.get("ts") == match_ts),
        None,
    )
    if match_index is None:
        return [
            "   Thread context did not include the matched message within the "
            f"{_MAX_THREAD_CONTEXT_MESSAGES}-message retrieval cap; open its permalink."
        ]

    start = max(0, match_index - display_limit // 2)
    start = min(start, max(0, len(messages) - display_limit))
    selected = messages[start:start + display_limit]

    lines = [f"  Thread context ({len(selected)} messages):"]
    for message in selected:
        user = await _resolve_user(message.get("user", ""))
        ts = message.get("ts", "")
        marker = "→" if ts == match_ts else " "
        text = _clean_search_text(message.get("text", "")) + _file_annotations(message)
        lines.append(f"  {marker} [{ts}] {user}: {text}")
    return lines


async def _handle_search_messages(args: dict) -> list[types.TextContent]:
    query_arg = args.get("query")
    if not isinstance(query_arg, str) or not query_arg.strip():
        return [types.TextContent(type="text", text="Error: query must be a non-empty string.")]
    query = query_arg.strip()
    if _search_client is None:
        return [types.TextContent(
            type="text",
            text=(
                "Error: search_messages requires SLACK_USER_TOKEN (xoxp) with the "
                "search:read user scope. Bot tokens cannot call Slack workspace search. "
                "Under OAuth & Permissions → User Token Scopes, add search:read plus the "
                "history/read/files/users scopes documented in the README, reinstall the "
                "app, then update SLACK_USER_TOKEN and restart the MCP server."
            ),
        )]

    limit, error = _bounded_integer_arg(args, "limit", 20, 1, 100)
    if error:
        return [types.TextContent(type="text", text=f"Error: {error}.")]
    page, error = _bounded_integer_arg(args, "page", 1, 1, 100)
    if error:
        return [types.TextContent(type="text", text=f"Error: {error}.")]
    thread_context_limit, error = _bounded_integer_arg(
        args, "thread_context_limit", 6, 1, 20,
    )
    if error:
        return [types.TextContent(type="text", text=f"Error: {error}.")]

    sort = args.get("sort", "score")
    sort_dir = args.get("sort_dir", "desc")
    include_context = args.get("include_thread_context", True)
    if not isinstance(sort, str) or sort not in {"score", "timestamp"}:
        return [types.TextContent(type="text", text="Error: sort must be score or timestamp.")]
    if not isinstance(sort_dir, str) or sort_dir not in {"asc", "desc"}:
        return [types.TextContent(type="text", text="Error: sort_dir must be asc or desc.")]
    if not isinstance(include_context, bool):
        return [types.TextContent(
            type="text", text="Error: include_thread_context must be a boolean.",
        )]

    try:
        response = await _search_client.search_messages(
            query=query,
            count=limit,
            page=page,
            sort=sort,
            sort_dir=sort_dir,
            highlight=False,
        )
    except Exception as e:
        response_data = getattr(getattr(e, "response", None), "data", {}) or {}
        if response_data.get("error") == "missing_scope":
            text = (
                "Error: SLACK_USER_TOKEN is missing search:read. Add search:read under "
                "OAuth & Permissions → User Token Scopes, reinstall the Slack app, replace "
                "SLACK_USER_TOKEN with the new xoxp token, and restart the MCP server."
            )
        elif response_data.get("error") == "not_allowed_token_type":
            text = (
                "Error: Slack workspace search requires a user xoxp token; bot xoxb tokens "
                "cannot call search.messages. Configure SLACK_USER_TOKEN with search:read."
            )
        else:
            text = f"Error: Slack message search failed: {response_data.get('error') or e}"
        return [types.TextContent(type="text", text=text)]

    payload = response.get("messages", {})
    matches = payload.get("matches", [])
    pagination = payload.get("pagination", {}) or payload.get("paging", {})
    total = payload.get("total", pagination.get("total_count", pagination.get("total", len(matches))))
    page_count = pagination.get("page_count", pagination.get("pages"))
    current_page = pagination.get("page", page)

    header = f"Found {total} matches; showing {len(matches)} on page {current_page}"
    if page_count:
        header += f" of {page_count}"
    header += f" (sort={sort} {sort_dir})."
    lines = [header]
    context_cache: dict[tuple[str, str], list[dict] | None] = {}

    for index, match in enumerate(matches, start=1):
        label, channel_id = await _search_channel_label(match)
        user_id = match.get("user", "")
        user = await _resolve_user(user_id) if user_id else (match.get("username") or "unknown")
        ts = match.get("ts", "")
        text = _clean_search_text(match.get("text", "")) + _file_annotations(match)
        permalink = match.get("permalink") or "unavailable"
        lines.extend([
            "",
            f"{index}. [{_format_slack_timestamp(ts)} · ts={ts}] {label} ({channel_id}) — {user}",
            f"   {text}",
            f"   permalink: {permalink}",
        ])

        root_ts = match.get("thread_ts") or (ts if match.get("reply_count") else None)
        if not root_ts:
            continue
        lines.append(
            f'   thread: use get_thread channel="{channel_id}" thread_ts="{root_ts}"'
        )
        key = (channel_id, root_ts)
        if not include_context:
            continue
        if key not in context_cache and len(context_cache) < _MAX_CONTEXT_THREADS:
            context_cache[key] = await _fetch_thread_messages(channel_id, root_ts)
        if key not in context_cache:
            lines.append("   Thread context omitted (five-thread response cap).")
        elif context_cache[key] is None:
            lines.append("   Thread context unavailable; use get_thread with the values above.")
        else:
            lines.extend(await _format_thread_context(
                context_cache[key] or [], ts, thread_context_limit,
            ))

    if not matches:
        lines.append("No messages matched this Slack search query.")
    elif page_count and current_page < min(page_count, 100):
        lines.extend(["", f"Next page: call search_messages again with page={current_page + 1}."])
    elif page_count and page_count > 100 and current_page >= 100:
        lines.extend([
            "",
            "Slack search exposes at most 100 pages; refine the query to reach more results.",
        ])
    return [types.TextContent(type="text", text="\n".join(lines))]


async def _handle_get_thread(args: dict) -> list[types.TextContent]:
    try:
        channel = await _resolve_channel_ref(args["channel"])
    except ValueError as e:
        return [types.TextContent(type="text", text=f"Error: {e}")]
    thread_ts = args["thread_ts"]
    result = await _read_client.conversations_replies(
        channel=channel, ts=thread_ts, limit=200,
    )

    lines = []
    for msg in result.get("messages", []):
        user = await _resolve_user(msg.get("user", ""))
        text = msg.get("text", "") + _file_annotations(msg)
        ts = msg.get("ts", "")
        lines.append(f"[{ts}] {user}: {text}")
    return [types.TextContent(type="text", text="\n".join(lines) or "No replies.")]


async def _handle_debug(args: dict) -> list[types.TextContent]:
    # Dump client_params if available
    client_info = None
    if _session and _session.client_params:
        cp = _session.client_params
        client_info = {
            "protocolVersion": cp.protocolVersion,
            "clientInfo": {"name": cp.clientInfo.name, "version": cp.clientInfo.version} if cp.clientInfo else None,
        }
        # Check for extra fields
        try:
            client_info["raw"] = cp.model_dump(mode="json")
        except Exception:
            pass
    info = {
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "conversation_id": _conversation_id,
        "bot_user_id": _bot_user_id,
        "owned_threads": _owned_threads,
        "channel_filter": CHANNEL_ID,
    }
    return [types.TextContent(type="text", text=json.dumps(info, indent=2))]


# ---------------------------------------------------------------------------
# User name resolution (cached)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# File attachments → on-demand download via the fetch_file tool
# ---------------------------------------------------------------------------
# Message renders never download anything. Each attached file (image, PDF, CSV,
# whatever) is annotated inline with its Slack file id, name, type, and size, so
# the agent sees what's there without any bytes hitting disk or context. When it
# actually wants a file, it calls the fetch_file tool with that id: the file is
# downloaded to a temp cache dir and its absolute path returned, which the agent
# then opens with its own Read tool. We use /tmp and don't manage retention: if
# the OS prunes a cached file, the next fetch_file just re-downloads it (keyed on
# cache-miss), so paths self-heal. Requires the read token's files:read scope;
# url_private downloads need an authenticated request.
_FILE_CACHE_DIR = Path(
    os.environ.get("SLACK_FILE_CACHE_DIR")
    or os.environ.get("SLACK_IMAGE_CACHE_DIR")  # back-compat with the images-only name
    or "/tmp/slack-channel-files"
)


def _human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.0f}GB"


def _safe_filename(file_id: str, name: str) -> str:
    base = re.sub(r"[^A-Za-z0-9._-]", "_", name or "file")
    return f"{file_id or 'file'}_{base}"


async def _download_slack_file(url: str) -> bytes | None:
    """Fetch a Slack url_private with the read token, off the event loop.

    Returns None on error, or when Slack serves its HTML sign-in page instead
    of the file (what happens when the token can't access the file).
    """
    token = getattr(_read_client, "token", None)

    def _get() -> bytes | None:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            ctype = resp.headers.get("Content-Type", "") or ""
            data = resp.read()
        if ctype.startswith("text/html"):
            return None
        return data

    try:
        return await anyio.to_thread.run_sync(_get)
    except Exception:
        logger.warning("Failed to download Slack file: %s", url, exc_info=True)
        return None


def _file_annotations(msg: dict) -> str:
    """Newline-prefixed notes for any files attached to a message.

    Purely descriptive — no download. Each file is listed with its type, size,
    and Slack id so the agent can pull it on demand via fetch_file. Returns ""
    when the message has no files.
    """
    files = msg.get("files") or []
    if not files:
        return ""
    lines: list[str] = []
    for f in files:
        name = f.get("name") or f.get("title") or "file"
        mimetype = f.get("mimetype", "") or ""
        kind = "image" if mimetype.startswith("image/") else "file"
        meta = mimetype or "unknown type"
        size = f.get("size") or 0
        if size:
            meta += f", {_human_size(size)}"
        file_id = f.get("id", "") or "?"
        lines.append(f"[{kind}: {name} ({meta}) · fetch_file id={file_id}]")
    return ("\n" + "\n".join(lines)) if lines else ""


async def _handle_fetch_file(args: dict) -> list[types.TextContent]:
    """Download a Slack file by id and return its local path for Read.

    Works for any attachment type — images, PDFs, CSVs, etc. Cache-hit returns
    the existing path without re-downloading.
    """
    file_id = (args.get("file_id") or "").strip()
    if not file_id:
        return [types.TextContent(type="text", text="Error: file_id is required.")]

    # Resolve the file's name + private URL from Slack (needs files:read).
    try:
        info = await _read_client.files_info(file=file_id)
        f = info["file"]
    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: could not look up file {file_id}: {e}")]

    name = f.get("name") or f.get("title") or "file"
    path = _FILE_CACHE_DIR / _safe_filename(file_id, name)

    # Cache hit — reuse.
    if path.exists() and path.stat().st_size > 0:
        return [types.TextContent(type="text", text=f"path={path}")]

    url = f.get("url_private_download") or f.get("url_private")
    data = await _download_slack_file(url) if url else None
    if not data:
        return [types.TextContent(
            type="text",
            text=f"Error: download failed for {name} (id={file_id}). "
                 "The read token may lack files:read or access to this file.",
        )]

    _FILE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return [types.TextContent(
        type="text",
        text=f"Downloaded {name} ({_human_size(len(data))}). Read it at:\npath={path}",
    )]


async def _resolve_user(user_id: str) -> str:
    if not user_id:
        return "unknown"
    if user_id in _user_name_cache:
        return _user_name_cache[user_id]
    if user_id == _bot_user_id:
        _user_name_cache[user_id] = "(bot)"
        return "(bot)"
    try:
        info = await _read_client.users_info(user=user_id)
        profile = info["user"]["profile"]
        # Fall back to the username before the raw ID: external / Slack Connect
        # Slack Connect users often have empty display_name/real_name but still
        # carry a username, which is what people type.
        name = (
            profile.get("display_name")
            or info["user"].get("real_name")
            or info["user"].get("name")
            or user_id
        )
        _user_name_cache[user_id] = name
        return name
    except Exception:
        return user_id


# ---------------------------------------------------------------------------
# Channel notifications → MCP client
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Cold replies (leader only) — for messages no active session owns
# ---------------------------------------------------------------------------

NO_RESPONSE_SENTINEL = "NO_RESPONSE"
REACT_PREFIX = "REACT:"


# ---------------------------------------------------------------------------
# Event bus: leader writes, all instances read
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Slack Socket Mode listener (leader only)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def _main() -> None:
    global _session, _slack_client, _read_client, _search_client, _bot_user_id

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stderr,
    )

    bot_token = os.environ.get("SLACK_BOT_TOKEN")

    if not bot_token:
        logger.error("SLACK_BOT_TOKEN is required")
        sys.exit(1)

    from slack_sdk.web.async_client import AsyncWebClient
    _slack_client = AsyncWebClient(token=bot_token)

    # Reads (history, channels, DMs) use the user token if provided so they see
    # everything the authorizing user can see, including DMs unavailable to the bot.
    # Writes and reactions stay on the bot token above.
    user_token = os.environ.get("SLACK_USER_TOKEN")
    _search_client = AsyncWebClient(token=user_token) if user_token else None
    _read_client = _search_client or _slack_client
    if user_token:
        logger.info("SLACK_USER_TOKEN set — reads use user (xoxp) token; writes use bot token")
    else:
        logger.info("SLACK_USER_TOKEN not set — reads fall back to bot token")

    # Resolve bot user ID (used to label our own messages)
    try:
        auth = await _slack_client.auth_test()
        _bot_user_id = auth["user_id"]
    except Exception:
        logger.warning("Could not resolve bot user ID", exc_info=True)

    async with stdio_server() as (read_stream, write_stream):
        init_options = InitializationOptions(
            server_name="slack-channel",
            server_version="0.1.0",
            capabilities=server.get_capabilities(
                notification_options=NotificationOptions(),
                # Required positionally as of mcp 1.29 — omitting it raises
                # TypeError at startup rather than at import, so the server
                # only fails once a client actually connects.
                experimental_capabilities={},
            ),
            instructions=(
                "Slack channel plugin — outbound only. Use the reply tool to post to a "
                "channel or thread, and read_history / search_messages / get_thread to read. This server "
                "does not listen for Slack messages and will never push anything to you; "
                "inbound Slack is handled outside this plugin. Threads you reply in are tracked per "
                "conversation and survive --resume."
            ),
        )

        async with AsyncExitStack() as stack:
            lifespan_ctx = await stack.enter_async_context(server.lifespan(server))
            session = await stack.enter_async_context(
                ServerSession(read_stream, write_stream, init_options)
            )
            _session = session

            async with anyio.create_task_group() as tg:
                # MCP message loop
                async for message in session.incoming_messages:
                    tg.start_soon(
                        server._handle_message,
                        message, session, lifespan_ctx, False,
                    )


def run() -> None:
    """CLI entrypoint."""
    anyio.run(_main)


if __name__ == "__main__":
    run()
