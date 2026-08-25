from types import SimpleNamespace

import pytest

from slack_channel import server


class FakeSearchClient:
    def __init__(self, response: dict | None = None, error: str | None = None):
        self.response = response or {}
        self.error = error
        self.calls: list[dict] = []

    async def search_messages(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            exc = RuntimeError("Slack API error")
            exc.response = SimpleNamespace(data={"error": self.error})
            raise exc
        return self.response


class FakeReadClient:
    def __init__(self):
        self.reply_calls: list[dict] = []

    async def users_info(self, user: str):
        return {
            "user": {
                "profile": {"display_name": {"U1": "Alice", "U2": "Bob"}.get(user, user)},
                "real_name": "",
                "name": user,
            },
        }

    async def conversations_replies(self, **kwargs):
        self.reply_calls.append(kwargs)
        return {
            "messages": [
                {"ts": "100.000001", "user": "U1", "text": "Thread root"},
                {"ts": "101.000001", "user": "U2", "text": "First matching reply"},
                {"ts": "102.000001", "user": "U1", "text": "Second matching reply"},
            ]
        }


@pytest.fixture(autouse=True)
def reset_search_state():
    old_search_client = server._search_client
    old_read_client = server._read_client
    server._search_client = None
    server._user_name_cache.clear()
    try:
        yield
    finally:
        server._search_client = old_search_client
        server._read_client = old_read_client
        server._user_name_cache.clear()


@pytest.mark.asyncio
async def test_search_tool_is_described_as_real_workspace_search():
    tools = {tool.name: tool for tool in await server.list_tools()}

    tool = tools["search_messages"]
    assert "Slack search syntax" in tool.description
    assert "rather than scanning recent history" in tool.description
    assert tool.inputSchema["required"] == ["query"]
    assert tool.inputSchema["properties"]["limit"]["maximum"] == 100


@pytest.mark.asyncio
async def test_search_formats_metadata_pagination_and_deduplicated_thread_context():
    search_client = FakeSearchClient(
        {
            "messages": {
                "total": 6,
                "pagination": {"page": 2, "page_count": 3, "total_count": 6},
                "matches": [
                    {
                        "channel": {"id": "CENG", "name": "engineering"},
                        "permalink": "https://example.slack.com/archives/CENG/p101000001",
                        "text": "First matching reply",
                        "thread_ts": "100.000001",
                        "ts": "101.000001",
                        "type": "message",
                        "user": "U2",
                    },
                    {
                        "channel": {"id": "CENG", "name": "engineering"},
                        "permalink": "https://example.slack.com/archives/CENG/p102000001",
                        "text": "Second matching reply",
                        "thread_ts": "100.000001",
                        "ts": "102.000001",
                        "type": "message",
                        "user": "U1",
                    },
                ],
            }
        }
    )
    read_client = FakeReadClient()
    server._search_client = search_client
    server._read_client = read_client

    [result] = await server._handle_search_messages(
        {
            "query": "launch in:engineering after:2026-01-01",
            "limit": 2,
            "page": 2,
            "sort": "timestamp",
            "sort_dir": "asc",
            "thread_context_limit": 2,
        }
    )

    assert search_client.calls == [
        {
            "query": "launch in:engineering after:2026-01-01",
            "count": 2,
            "page": 2,
            "sort": "timestamp",
            "sort_dir": "asc",
            "highlight": False,
        }
    ]
    assert read_client.reply_calls == [
        {"channel": "CENG", "ts": "100.000001", "limit": 100}
    ]
    assert "Found 6 matches; showing 2 on page 2 of 3 (sort=timestamp asc)." in result.text
    assert "#engineering (CENG) — Bob" in result.text
    assert "1970-01-01 00:01:41 UTC · ts=101.000001" in result.text
    assert "permalink: https://example.slack.com/archives/CENG/p101000001" in result.text
    assert 'get_thread channel="CENG" thread_ts="100.000001"' in result.text
    assert "→ [101.000001] Bob: First matching reply" in result.text
    assert "→ [102.000001] Alice: Second matching reply" in result.text
    assert "Next page: call search_messages again with page=3." in result.text


@pytest.mark.asyncio
async def test_search_without_user_token_returns_actionable_setup_error():
    server._search_client = None

    [result] = await server._handle_search_messages({"query": "roadmap"})

    assert "SLACK_USER_TOKEN (xoxp)" in result.text
    assert "search:read" in result.text
    assert "reinstall" in result.text
    assert "Bot tokens cannot" in result.text


@pytest.mark.asyncio
async def test_search_missing_scope_returns_reauthorization_steps():
    server._search_client = FakeSearchClient(error="missing_scope")

    [result] = await server._handle_search_messages({"query": "roadmap"})

    assert "missing search:read" in result.text
    assert "User Token Scopes" in result.text
    assert "reinstall the Slack app" in result.text
    assert "replace SLACK_USER_TOKEN" in result.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "expected"),
    [
        ({"query": "roadmap", "limit": 0}, "limit must be an integer from 1 to 100"),
        ({"query": "roadmap", "page": 101}, "page must be an integer from 1 to 100"),
        (
            {"query": "roadmap", "thread_context_limit": True},
            "thread_context_limit must be an integer from 1 to 20",
        ),
        ({"query": "roadmap", "sort": "newest"}, "sort must be score or timestamp"),
        ({"query": 123}, "query must be a non-empty string"),
        (
            {"query": "roadmap", "include_thread_context": "false"},
            "include_thread_context must be a boolean",
        ),
    ],
)
async def test_search_validates_bounded_arguments(args, expected):
    server._search_client = FakeSearchClient()

    [result] = await server._handle_search_messages(args)

    assert expected in result.text


@pytest.mark.asyncio
async def test_thread_context_fetch_follows_cursor_pagination():
    class PaginatedReadClient:
        def __init__(self):
            self.calls = []

        async def conversations_replies(self, **kwargs):
            self.calls.append(kwargs)
            if "cursor" not in kwargs:
                return {
                    "messages": [{"ts": "100.000001", "text": "root"}],
                    "response_metadata": {"next_cursor": "next-page"},
                }
            return {
                "messages": [{"ts": "101.000001", "text": "reply"}],
                "response_metadata": {"next_cursor": ""},
            }

    read_client = PaginatedReadClient()
    server._read_client = read_client

    messages = await server._fetch_thread_messages("CENG", "100.000001")

    assert [message["ts"] for message in messages] == ["100.000001", "101.000001"]
    assert read_client.calls == [
        {"channel": "CENG", "ts": "100.000001", "limit": 100},
        {"channel": "CENG", "ts": "100.000001", "limit": 100, "cursor": "next-page"},
    ]


@pytest.mark.asyncio
async def test_thread_context_does_not_show_unrelated_messages_when_match_is_outside_cap():
    lines = await server._format_thread_context(
        [{"ts": "100.000001", "text": "unrelated root"}],
        match_ts="999.000001",
        display_limit=6,
    )

    rendered = "\n".join(lines)
    assert "did not include the matched message" in rendered
    assert "open its permalink" in rendered
    assert "unrelated root" not in rendered


@pytest.mark.asyncio
async def test_page_100_does_not_advertise_rejected_page_101():
    server._search_client = FakeSearchClient(
        {
            "messages": {
                "total": 10001,
                "pagination": {"page": 100, "page_count": 101},
                "matches": [{
                    "channel": {"id": "CENG", "name": "engineering"},
                    "permalink": "https://example.slack.com/archives/CENG/p100",
                    "text": "last reachable result",
                    "ts": "100.000001",
                    "type": "message",
                    "user": "U1",
                }],
            }
        }
    )
    server._read_client = FakeReadClient()

    [result] = await server._handle_search_messages({"query": "roadmap", "page": 100})

    assert "page=101" not in result.text
    assert "at most 100 pages" in result.text
    assert "refine the query" in result.text
