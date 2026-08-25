---
name: configure
description: Configure Slack credentials for the slack-channel plugin
user-invocable: true
allowed-tools:
  - Read
  - Write
  - Bash(ls *)
  - Bash(mkdir *)
---

# Slack Channel Plugin Configuration

Help the user configure credentials at:

`~/.claude/channels/slack-channel/.env`

## Show status when no arguments are provided

Report whether each value is set, with token values masked:

- `SLACK_BOT_TOKEN` — required `xoxb` token for messages and reactions
- `SLACK_USER_TOKEN` — recommended `xoxp` token for broad reads; required for workspace search
- `SLACK_CHANNEL_ID` — optional default channel for `reply`

`SLACK_APP_TOKEN` is not used. This outbound-only MCP server does not use Socket Mode.

## Save credentials

Preserve existing values the user did not ask to change. Write the requested values in dotenv
format:

```dotenv
SLACK_BOT_TOKEN=xoxb-...
SLACK_USER_TOKEN=xoxp-...
SLACK_CHANNEL_ID=C...  # optional
```

Never put tokens in the repository. Restart Claude Code or the MCP host after changing the file.

## Clear credentials

If the user says `clear`, remove `~/.claude/channels/slack-channel/.env`.

## Slack app setup

1. Create or select an app at <https://api.slack.com/apps>.
2. Under **OAuth & Permissions → Bot Token Scopes**, add:
   - `chat:write`
   - `reactions:write`
   - `channels:history`, `channels:read`
   - `groups:history`, `groups:read`
   - `im:history`, `im:read`
   - `mpim:history`, `mpim:read`
   - `files:read`, `users:read`
3. Under **OAuth & Permissions → User Token Scopes**, add:
   - `search:read`
   - `channels:history`, `channels:read`
   - `groups:history`, `groups:read`
   - `im:history`, `im:read`
   - `mpim:history`, `mpim:read`
   - `files:read`, `users:read`
4. Click **Install to Workspace** or **Reinstall to Workspace** and approve the scopes.
5. Copy the **Bot User OAuth Token** (`xoxb-...`) into `SLACK_BOT_TOKEN`.
6. Copy the **User OAuth Token** (`xoxp-...`) into `SLACK_USER_TOKEN`.
7. Restart Claude Code or the MCP host.

## Search authorization

Slack workspace search uses `search.messages`, which only accepts a user `xoxp` token with
`search:read`. A bot `xoxb` token cannot search workspace history. If search reports
`missing_scope`, add `search:read` under **User Token Scopes**, reinstall the app, copy the resulting
User OAuth Token, update `SLACK_USER_TOKEN`, and restart the MCP server.
