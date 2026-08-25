# slack-channel-plugin

Outbound Slack MCP server for Claude Code and other MCP clients. It sends messages and
reactions, reads conversations and files, and searches workspace message history. It does
not listen for inbound Slack events; QM handles inbound Slack.

## Capabilities

- Bot-authored messages and reactions
- Reads across the channels and DMs visible to the configured Slack user
- True workspace-wide historical search through Slack's `search.messages` API
- Human-readable channel and DM references such as `#engineering`, `@Yue`, and `yufan`
- On-demand download of Slack file attachments
- Per-conversation tracking of threads created through `reply`

## Install

```bash
# From a marketplace (once published)
claude plugin install slack-channel@<marketplace>

# Or during development
claude --plugin-dir /path/to/slack-channel-plugin
```

## Slack app setup

Create an app at <https://api.slack.com/apps> using this manifest:

```yaml
display_information:
  name: Claude Code
oauth_config:
  scopes:
    bot:
      - channels:history
      - channels:read
      - chat:write
      - files:read
      - groups:history
      - groups:read
      - im:history
      - im:read
      - mpim:history
      - mpim:read
      - reactions:write
      - users:read
    user:
      - channels:history
      - channels:read
      - files:read
      - groups:history
      - groups:read
      - im:history
      - im:read
      - mpim:history
      - mpim:read
      - search:read
      - users:read
features:
  bot_user:
    display_name: Claude Code
```

Install the app to the workspace, then copy both OAuth tokens:

- **Bot User OAuth Token** (`xoxb-...`): used for writes and reactions, and as the read
  fallback when no user token is configured.
- **User OAuth Token** (`xoxp-...`): used for reads and workspace-wide search. Search requires
  the user-only `search:read` scope; Slack bot tokens cannot call `search.messages`.

An App-Level Token (`xapp-...`) and Socket Mode are not required because this server does not
receive inbound events.

## Configure

Create `~/.claude/channels/slack-channel/.env`:

```dotenv
SLACK_BOT_TOKEN=xoxb-...
SLACK_USER_TOKEN=xoxp-...
SLACK_CHANNEL_ID=C...  # optional default for reply
```

Real environment variables take precedence. The server also reads `.env` from the package and
its parent directory for local development.

Start Claude Code with the channel MCP enabled:

```bash
claude --dangerously-load-development-channels server:slack-channel
```

### Enable search on an existing installation

Slack's historical message search is user-authorized. To add it to an existing app:

1. Open <https://api.slack.com/apps>, select the app, and open **OAuth & Permissions**.
2. Under **Scopes → User Token Scopes**, add `search:read`.
3. Click **Reinstall to Workspace** at the top of the page and approve the new permission.
4. Copy the resulting **User OAuth Token** (`xoxp-...`). Do not use the Bot User OAuth Token.
5. Set `SLACK_USER_TOKEN=xoxp-...` in `~/.claude/channels/slack-channel/.env`.
6. Restart Claude Code or the MCP host so the server reloads the token.

Without `SLACK_USER_TOKEN`, `search_messages` returns an actionable configuration error. If the
user token exists but predates the new scope, Slack returns `missing_scope`; reinstalling the app
and updating the token completes the authorization.

## MCP tools

| Tool | Description |
|------|-------------|
| `reply` | Send a bot-authored message to a channel, DM, group DM, or thread |
| `add_reaction` | Add an emoji reaction |
| `remove_reaction` | Remove an emoji reaction |
| `list_channels` | List channels and DMs visible to the read token |
| `read_history` | Read recent messages from one conversation |
| `search_messages` | Search historical messages across the workspace with native Slack query syntax |
| `get_thread` | Read replies in one thread |
| `fetch_file` | Download an attached Slack file by file ID |
| `debug` | Show internal server state |

### Search messages

`search_messages` passes `query` directly to Slack. Native modifiers can be combined:

```text
roadmap
"clinical validation" in:project-lattice
from:alice after:2026-07-01 before:2026-08-01
has:link in:engineering
```

Inputs:

| Field | Default | Notes |
|-------|---------|-------|
| `query` | required | Native Slack search query |
| `limit` | `20` | Results per page, 1–100 |
| `page` | `1` | 1-indexed page, 1–100 |
| `sort` | `score` | `score` or `timestamp` |
| `sort_dir` | `desc` | `asc` or `desc` |
| `include_thread_context` | `true` | Includes context for up to five distinct matched threads |
| `thread_context_limit` | `6` | Messages shown per included thread, 1–20 |

Each result includes the UTC time, Slack timestamp, human channel/DM label, channel ID, author,
message text, permalink, and actionable `get_thread` arguments when relevant. Thread context is
bounded to keep large searches readable. Pagination metadata and the next page number are included
in the response.

This tool calls Slack's workspace search endpoint. It never approximates historical search by
scanning `conversations.history`, so results cover all indexed messages visible to the authorizing
user, subject to Slack's own search filters and retention policy.

### Human channel references

Tools that take a `channel` argument accept human-readable references and Slack IDs:

| Input form | Example |
|------------|---------|
| Channel label | `#engineering` |
| Channel name | `engineering` |
| DM label | `@Yue` |
| Person name | `Yue` |
| Distinctive DM/group-DM substring | `yufan` |
| Slack ID | `C0AAWT14XT4` |

Prefer names in agent workflows. `list_channels` prints copyable `use channel="..."` labels; IDs
remain available for debugging and exact thread calls.

## Architecture and authentication

- `_slack_client` always uses `SLACK_BOT_TOKEN` for writes and reactions.
- `_read_client` uses `SLACK_USER_TOKEN` when configured and otherwise falls back to the bot token.
- `_search_client` exists only when `SLACK_USER_TOKEN` is configured. Slack restricts
  `search.messages` and `search:read` to user tokens.
- The SessionStart hook records the stable Claude Code conversation ID. Threads created through
  `reply` are persisted for seven days under `~/.config/slack-channel/`.
- The stdio MCP server has no Socket Mode connection, event bus, or passive listener.

## Development

```bash
uv sync --extra dev
uv run python -m pytest -q
uv run ruff check slack_channel/ tests/
uv build
```

## License

Apache 2.0
