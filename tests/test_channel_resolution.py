import pytest

from slack_channel import server


class FakeReadClient:
    def __init__(self, channels: list[dict], users: dict[str, str] | None = None):
        self.channels = channels
        self.users = users or {}

    async def conversations_list(self, **kwargs):
        return {
            "channels": self.channels,
            "response_metadata": {"next_cursor": ""},
        }

    async def users_info(self, user: str):
        # External / Slack Connect users often have empty display_name and
        # real_name but still carry a username. Model that: `users` maps a
        # user id to the username, exposed only via the top-level "name".
        username = self.users.get(user, user)
        return {
            "user": {
                "profile": {"display_name": "", "real_name": ""},
                "real_name": "",
                "name": username,
            },
        }


@pytest.fixture(autouse=True)
def reset_server_state():
    old_read_client = server._read_client
    old_bot_user_id = server._bot_user_id
    server._bot_user_id = None
    server._user_name_cache.clear()
    try:
        yield
    finally:
        server._read_client = old_read_client
        server._bot_user_id = old_bot_user_id
        server._user_name_cache.clear()


@pytest.mark.asyncio
async def test_channel_id_passthrough():
    server._read_client = FakeReadClient([])

    assert await server._resolve_channel_ref("C123ABC") == "C123ABC"
    assert await server._resolve_channel_ref("D123ABC") == "D123ABC"
    assert await server._resolve_channel_ref("G123ABC") == "G123ABC"


@pytest.mark.asyncio
async def test_resolves_direct_dm_by_display_name():
    server._read_client = FakeReadClient(
        [{"id": "D111", "is_im": True, "user": "U_ALICE"}],
        users={"U_ALICE": "Alice"},
    )

    assert await server._resolve_channel_ref("@Alice") == "D111"
    assert await server._resolve_channel_ref("Alice") == "D111"


@pytest.mark.asyncio
async def test_resolves_group_dm_by_distinctive_person_substring():
    server._read_client = FakeReadClient(
        [
            {
                "id": "G111",
                "name": "mpdm-owner--bob--carol.smith-1",
                "is_mpim": True,
            }
        ]
    )

    assert await server._resolve_channel_ref("carol") == "G111"


@pytest.mark.asyncio
async def test_prefers_direct_dm_over_group_dm_for_external_user():
    # An external Slack Connect user may have only a username. Typing their
    # distinctive name should choose the direct DM over a matching group DM.
    server._read_client = FakeReadClient(
        [
            {"id": "D222", "is_im": True, "user": "U_CAROL"},
            {
                "id": "G111",
                "name": "mpdm-owner--bob--carol.smith-1",
                "is_mpim": True,
            },
        ],
        users={"U_CAROL": "carol.smith"},
    )

    assert await server._resolve_channel_ref("carol") == "D222"
    assert await server._resolve_channel_ref("carol.smith") == "D222"


@pytest.mark.asyncio
async def test_ambiguous_fuzzy_match_lists_human_labels():
    server._read_client = FakeReadClient(
        [
            {"id": "G111", "name": "mpdm-owner--bob--carol.smith-1", "is_mpim": True},
            {"id": "G222", "name": "mpdm-owner--bob--alice-1", "is_mpim": True},
        ]
    )

    with pytest.raises(ValueError, match="Ambiguous channel or DM 'bob'"):
        await server._resolve_channel_ref("bob")


@pytest.mark.asyncio
async def test_list_channels_shows_copyable_name_labels():
    server._read_client = FakeReadClient(
        [
            {"id": "CENG", "name": "engineering", "num_members": 3},
            {"id": "D333", "is_im": True, "user": "U_ALICE"},
        ],
        users={"U_ALICE": "Alice"},
    )

    [result] = await server._handle_list_channels({"limit": 20})

    assert 'use channel="#engineering"' in result.text
    assert 'use channel="@Alice"' in result.text
