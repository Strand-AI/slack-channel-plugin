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
        [{"id": "D0A0N5DCSMQ", "is_im": True, "user": "U_YUE"}],
        users={"U_YUE": "Yue"},
    )

    assert await server._resolve_channel_ref("@Yue") == "D0A0N5DCSMQ"
    assert await server._resolve_channel_ref("Yue") == "D0A0N5DCSMQ"


@pytest.mark.asyncio
async def test_resolves_group_dm_by_distinctive_person_substring():
    server._read_client = FakeReadClient(
        [
            {
                "id": "C0BG0HEBHFX",
                "name": "mpdm-oded--peter--yufan.liu-1",
                "is_mpim": True,
            }
        ]
    )

    assert await server._resolve_channel_ref("yufan") == "C0BG0HEBHFX"


@pytest.mark.asyncio
async def test_prefers_direct_dm_over_group_dm_for_external_user():
    # Oded's 1:1 DM with Yufan (external Slack Connect user: only a username,
    # no display/real name) plus a group DM whose name contains "yufan.liu".
    # Typing "yufan" must land on the DM, not the group DM.
    server._read_client = FakeReadClient(
        [
            {"id": "D0A8HGFTGJ1", "is_im": True, "user": "U_YUFAN"},
            {
                "id": "C0BG0HEBHFX",
                "name": "mpdm-oded--peter--yufan.liu-1",
                "is_mpim": True,
            },
        ],
        users={"U_YUFAN": "yufan.liu"},
    )

    assert await server._resolve_channel_ref("yufan") == "D0A8HGFTGJ1"
    assert await server._resolve_channel_ref("yufan.liu") == "D0A8HGFTGJ1"


@pytest.mark.asyncio
async def test_ambiguous_fuzzy_match_lists_human_labels():
    server._read_client = FakeReadClient(
        [
            {"id": "C111", "name": "mpdm-oded--peter--yufan.liu-1", "is_mpim": True},
            {"id": "C222", "name": "mpdm-oded--peter--yue-1", "is_mpim": True},
        ]
    )

    with pytest.raises(ValueError, match="Ambiguous channel or DM 'peter'"):
        await server._resolve_channel_ref("peter")


@pytest.mark.asyncio
async def test_list_channels_shows_copyable_name_labels():
    server._read_client = FakeReadClient(
        [
            {"id": "CENG", "name": "engineering", "num_members": 3},
            {"id": "DYUE", "is_im": True, "user": "U_YUE"},
        ],
        users={"U_YUE": "Yue"},
    )

    [result] = await server._handle_list_channels({"limit": 20})

    assert 'use channel="#engineering"' in result.text
    assert 'use channel="@Yue"' in result.text
