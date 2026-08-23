# Setup

Everything needed to go from nothing to talking to your own machine from your
phone. No prior knowledge of this server is assumed; if you have never made a
Slack app before, start at the top and work down.

The end state is: a private Slack channel, a Slack app you own that is a member
of it, and a resident Claude CLI session on your machine that reads that
channel through this server.

1. [Create the Slack app](#1-create-the-slack-app)
2. [Create the channel and collect the two IDs](#2-create-the-channel-and-collect-the-two-ids)
3. [Install the server](#3-install-the-server)
4. [Set the environment variables](#4-set-the-environment-variables)
5. [Register it with Claude Code](#5-register-it-with-claude-code)
6. [First run](#6-first-run)
7. [Asking you a question](#asking-you-a-question)
8. [Troubleshooting](#troubleshooting)

## 1. Create the Slack app

The app is yours alone: it exists to sit in one private channel and relay your
own messages. It needs three bot scopes, one event subscription, two settings
toggles, and two tokens.

### The quick way: from the manifest

Go to [api.slack.com/apps](https://api.slack.com/apps) → **Create New App** →
**From an app manifest**, pick your workspace, and paste
[docs/slack-app-manifest.yaml](slack-app-manifest.yaml). That sets Socket Mode,
interactivity, the scopes and the event subscription in one step. Then continue
at [Generate the two tokens](#generate-the-two-tokens).

### The manual way

**Create New App** → **From scratch**, name it, pick your workspace. Then:

1. **Socket Mode** → turn it **on**. Socket Mode is what lets the server hold a
   WebSocket out to Slack instead of running an HTTP endpoint Slack can reach,
   which is the only workable shape for something running on a laptop behind
   NAT.
2. **OAuth & Permissions** → **Bot Token Scopes**, add exactly these:

   | Scope | Why the server needs it |
   |---|---|
   | `chat:write` | `chat.postMessage` for replies, plus `chat.update` and `chat.delete` for the [processing indicator](../README.md#processing-indicator) |
   | `groups:history` | `conversations.history`, which is how the bridge catches up on everything sent while your machine was asleep. Use `channels:history` instead if your bridge channel is public |
   | `reactions:write` | `reactions.add`, which is all `slack_ack` does |

   Nothing else is called. There is no user token, and the server never reads a
   channel it is not bound to.
3. **Event Subscriptions** → turn it **on** and subscribe to the bot event
   `message.groups` (or `message.channels` for a public channel). This is the
   live half; history covers the rest.
4. **Interactivity & Shortcuts** → turn it **on**. Leave the request URL empty:
   with Socket Mode the button clicks come down the same WebSocket as the
   messages, and Slack does not ask for a URL. This is what
   [`slack_ask`](../README.md#asking-the-owner-a-question) needs; without it
   Slack simply never delivers the clicks.

Interactivity is a setting rather than a scope, so an app installed before this
was added only needs the toggle flipped — no reinstall, no new token.

### Generate the two tokens

- **App-level token** — **Basic Information** → **App-Level Tokens** →
  **Generate Token and Scopes**, add the `connections:write` scope. The
  `xapp-…` value it shows you is `SLACK_APP_TOKEN`. It is shown once.
- **Bot token** — **OAuth & Permissions** → **Install to Workspace** and
  approve. The **Bot User OAuth Token** (`xoxb-…`) is `SLACK_BOT_TOKEN`.

If you change scopes later, reinstall the app; the existing token does not
gain them on its own.

## 2. Create the channel and collect the two IDs

Create a **private channel** for the conversation — `#agent` or similar — and
invite the app to it with `/invite @your-app-name`. A bot can only read
channels it is a member of, so this step is not optional.

Then collect:

- **Channel ID** (`C…`) — click the channel name → **View channel details** →
  scroll to the bottom of the **About** tab. This is `SLACK_BRIDGE_CHANNEL`.
- **Your user ID** (`U…`) — your avatar → **Profile** → **⋮** → **Copy member
  ID**. This is `SLACK_BRIDGE_OWNER`, and only messages from this user are
  relayed to the agent.

Keep the channel private and to yourself. Messages arriving over the bridge
reach an agent with tool access on your machine; see
[Security](../README.md#security).

## 3. Install the server

Homebrew:

```sh
brew install ngs/tap/slack-bridge-mcp-server
```

Go toolchain:

```sh
go install go.ngs.io/slack-bridge-mcp-server@latest
```

Or download an archive for your platform from
[releases](https://github.com/ngs/slack-bridge-mcp-server/releases) and put the
binary on your `PATH`. Debian/RPM/Alpine packages are published there too.

Check it runs — this prints the version and exits, and needs no configuration:

```sh
slack-bridge-mcp-server --version
```

## 4. Set the environment variables

| Variable | Required | Value |
|---|---|---|
| `SLACK_BOT_TOKEN` | yes | the `xoxb-…` bot token |
| `SLACK_APP_TOKEN` | yes | the `xapp-…` app-level token |
| `SLACK_BRIDGE_CHANNEL` | yes | the `C…` channel ID |
| `SLACK_BRIDGE_OWNER` | yes | your `U…` user ID |
| `SLACK_BRIDGE_STATE_DIR` | no | overrides where the cursor and lock live |
| `SLACK_BRIDGE_INDICATOR` | no | `off` disables the processing indicator |
| `SLACK_BRIDGE_INDICATOR_GRACE` | no | seconds before it appears; default 10, clamped to 3–120 |
| `SLACK_BRIDGE_INDICATOR_INTERVAL` | no | seconds between updates; default 10, clamped to 5–60 |

Tokens are read from the environment and are never logged or written to the
state file, so keeping them out of the repository is worth the small effort. A
[direnv](https://direnv.net) `.envrc`, gitignored, is one way:

```sh
# .envrc — add .envrc to .gitignore, then run: direnv allow
export SLACK_BOT_TOKEN="xoxb-..."
export SLACK_APP_TOKEN="xapp-..."
export SLACK_BRIDGE_CHANNEL="C0123456789"
export SLACK_BRIDGE_OWNER="U0123456789"
```

The alternative is putting them in the `env` block of `.mcp.json` below, which
is simpler but means the file holds secrets.

## 5. Register it with Claude Code

Add the server to `.mcp.json` in the project you want to run the resident
session from:

```json
{
  "mcpServers": {
    "slack-bridge": {
      "type": "stdio",
      "command": "slack-bridge-mcp-server",
      "env": {
        "SLACK_BOT_TOKEN": "xoxb-...",
        "SLACK_APP_TOKEN": "xapp-...",
        "SLACK_BRIDGE_CHANNEL": "C0123456789",
        "SLACK_BRIDGE_OWNER": "U0123456789"
      }
    }
  }
}
```

Drop the `env` block if the variables are already in the session's environment,
via direnv or your shell profile.

Restart the CLI and ask it to call `slack_status`. On a fresh session it
answers `"connected": false`, and **that is the expected answer**: the server
does not open a socket until the first tool call that actually needs Slack —
`slack_wait`, `slack_post`, `slack_ack` or `slack_ask`, whichever you reach
first — so that it can sit in every project's `.mcp.json` without connecting in
sessions that never use it. `slack_status` itself never connects, which is what
makes it useful when something is misconfigured. What matters at this point is
that `config_error` is absent — if it is there, it names the variables that did
not arrive.

## 6. First run

Ask the session to call `slack_wait`, then send a message to the channel from
Slack. It should come back to the agent within a second or two. From then on,
run the loop in
[Running a resident session](../README.md#running-a-resident-session).

Three behaviours are worth knowing before you wonder about them:

- **Your old messages are not replayed.** The very first `slack_wait` against a
  channel records the newest existing message as the starting point and returns
  nothing. A fresh install joins the conversation rather than dumping the
  channel's history into the model's context. Everything sent *after* that
  point is delivered.
- **The cursor is a file, not memory.** It lives in
  `~/.config/slack-bridge/state.json` (honouring `XDG_CONFIG_HOME`) and moves
  only once messages have actually been handed to the agent. A crash costs you
  a duplicate, never a lost message.
- **One session at a time.** The first `slack_wait` takes an exclusive `flock`
  on `~/.config/slack-bridge/bridge.lock`. A second bridge would split your
  messages between two listeners, each seeing half the conversation, so it
  refuses to start instead. The operating system drops the lock when the
  process exits, including on a crash, so a stale lock file is never something
  you have to clean up.

## Asking you a question

`slack_ask` is the other direction: the agent posts a question with a button per
answer and waits for you to tap one.

```
Ship the fix now, or hold it until the release?

[ Ship now ]  [ Hold ]  [ Ask me later ]
```

The buttons disappear as soon as you tap one — the message is rewritten to show
what you chose — and after the timeout, if you never do. The tool needs
**Interactivity** enabled on the app (step 1); everything else it uses is
already set up. One question can be outstanding at a time.

## Troubleshooting

**`slack_status` reports a `config_error`.** It names every variable that is
unset. The process starts anyway, precisely so it can tell you this; the other
tools fail with the same message.

**"another slack-bridge session is already waiting on this channel".** Another
CLI session has the bridge open — often one you forgot in a different terminal
or tmux window. Close it, and the next `slack_wait` takes over. If you are
certain nothing else is running, check for an orphaned
`slack-bridge-mcp-server` process.

**`authenticating with Slack: ...` on the first call.** The bot token is wrong,
revoked, or from a different workspace. The server checks with `auth.test` at
connect time rather than letting a bad token surface later as an empty history.

**Messages are sent but never arrive.** In order of likelihood: the app is not
in the channel (`/invite` it), `SLACK_BRIDGE_CHANNEL` is a different channel
than the one you are typing in, `SLACK_BRIDGE_OWNER` is not your user ID, or
the event subscription is `message.channels` while the channel is private (or
the reverse). Only plain messages and thread replies from that one user are
relayed — edits, deletions, joins and anything posted by a bot are dropped on
purpose, the last of these so the agent never reads its own replies back as new
instructions.

**Nothing came through while the laptop was asleep.** It will. The WebSocket
dies with the network, and on reconnect the bridge re-reads the window after
its cursor from `conversations.history` and merges it with whatever the socket
delivered, deduplicating by timestamp. The backlog arrives on the next
`slack_wait` as a single array.

**Tapping a `slack_ask` button does nothing.** Interactivity is off on the app.
Turn it on under **Interactivity & Shortcuts** — it is a setting, not a scope,
so nothing needs reinstalling — and ask again.

**Diagnostics.** Everything the server logs goes to stderr, prefixed
`slack-bridge:`, because stdout carries the JSON-RPC stream and a stray byte
there breaks the session. Look in the CLI's MCP log for that prefix.
