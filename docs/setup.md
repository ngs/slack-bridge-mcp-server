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
7. [Reading the channel](#reading-the-channel)
8. [Asking you a question](#asking-you-a-question)
9. [Troubleshooting](#troubleshooting)

## 1. Create the Slack app

The app is yours alone: it exists to relay your own messages, from your home
channel and from any conversation you open by mentioning it elsewhere. It needs
a handful of bot scopes, three event subscriptions, two settings toggles, and
two tokens.

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
   | `groups:history` | `conversations.history` and `conversations.replies` in private channels, which is how the bridge catches up on everything sent while your machine was asleep |
   | `channels:history` | The same in public channels. Needed if your home channel is public, and for any public channel you add the app to |
   | `channels:read`, `groups:read` | `users.conversations`, which is how the search for [mentions](../README.md#talking-to-the-agent-in-other-channels) sent while you were away knows which channels to look in |
   | `reactions:write` | `reactions.add`, for the automatic 👀 receipt and for `slack_ack` |
   | `users:read` | `users.info`, which turns user IDs into names in [`slack_history`](../README.md#reading-the-channel) results. The only optional one: without it the tool falls back to raw IDs |

   Nothing else is called. There is no user token, and the server never reads a
   channel it is not bound to.
3. **Event Subscriptions** → turn it **on** and subscribe to the bot events
   `message.groups` and `message.channels` — the live half, for the home
   channel — and `app_mention`, which is how the app hears the mention that
   opens a conversation in any other channel. History covers what the socket
   misses.
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
gain them on its own. Two upgrades need one:

- `users:read`, if you are coming from a version before `slack_history` existed
  — though that tool works without it too, showing raw user IDs instead of
  names, so the reinstall can wait until it suits you.
- `channels:read` and `groups:read`, plus `channels:history` and
  `groups:history` for the public and private channels it should be reading, if
  you are coming from a version before conversations outside the home channel
  existed. Without them the home channel works exactly as before and nothing
  else does, which is a perfectly good place to stay until you want the rest;
  the server says once on stderr which scope it was refused.

Event subscriptions and interactivity, by contrast, are settings rather than
scopes: adding `app_mention` or turning interactivity on needs no reinstall.

## 2. Create the channel and collect the two IDs

Create a **private channel** for the conversation — `#agent` or similar — and
invite the app to it with `/invite @your-app-name`. A bot can only read
channels it is a member of, so this step is not optional.

This is the **home channel**: everything you say in it reaches the agent. You
can invite the app to other channels later and talk to it there by mentioning
it — see [Talking to the agent in other
channels](../README.md#talking-to-the-agent-in-other-channels) — but the home
channel is the one the bridge is configured with and the one it guarantees.

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
| `SLACK_BRIDGE_AUTO_ACK` | no | `off` disables the automatic 👀 receipt reaction |
| `SLACK_BRIDGE_AUTO_ACK_EMOJI` | no | emoji for the receipt reaction, without colons; default `eyes` |
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
`slack_wait`, `slack_post`, `slack_ack`, `slack_ask`, `slack_history`, or
`slack_progress` when it has to start an indicator, whichever you reach first — so that it can sit in every project's `.mcp.json`
without connecting in
sessions that never use it. `slack_status` itself never connects, which is what
makes it useful when something is misconfigured. What matters at this point is
that `config_error` is absent — if it is there, it names the variables that did
not arrive.

## 6. First run

Ask the session to call `slack_wait`, then send a message to the channel from
Slack. It should come back to the agent within a second or two. From then on,
run the loop in
[Running a resident session](../README.md#running-a-resident-session).

Four behaviours are worth knowing before you wonder about them:

- **Your message gets a 👀 straight away.** The server adds it as soon as the
  message reaches the session, before the model has read a word of it, so the
  reaction means "delivered" and the ⏳ that may follow means "still working".
  See [Receipt and progress](../README.md#receipt-and-progress).
- **Your old messages are not replayed.** The very first `slack_wait` against a
  channel records where the conversation already is — the newest message or
  thread reply — as its starting point, and returns nothing. A fresh install joins the conversation rather than dumping the
  channel's history into the model's context. Everything sent *after* that
  point is delivered.
- **The cursor is a file, not memory.** It lives in
  `~/.config/slack-bridge/state.json` (honouring `XDG_CONFIG_HOME`) and moves
  only as messages are handed over, so a restart picks up where the last
  session left off instead of replaying the channel or starting from now. It is
  not a delivery receipt: the cursor is written just before the messages reach
  the session, so a crash in that instant leaves them behind. Anything that
  matters is still in Slack, where you can see it and say it again.
- **One session at a time.** The first tool call that connects to Slack —
  usually `slack_wait`, but any of them will do it — takes an exclusive `flock`
  on `~/.config/slack-bridge/bridge.lock`. A second bridge would split your
  messages between two listeners, each seeing half the conversation, so it
  refuses to start instead. The operating system drops the lock when the
  process exits, including on a crash, so a stale lock file is never something
  you have to clean up.

## Reading the channel

When you ask the agent to read or summarise the channel, it calls
`slack_history`, which returns everyone's messages — colleagues, bots, incoming
webhooks — rather than only yours. That is the point: the discussion worth
summarising is usually the one you had with other people.

It only reads. Nothing it fetches is consumed from the relay, so a message you
sent while the agent was busy is still waiting for the next `slack_wait`.

Names come from `users.info` and need the `users:read` scope. Without it you get
raw user IDs and everything else works, so upgrading an existing app can wait
for a convenient moment.

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
`slack_wait` as a single array. Replies you typed inside a thread come back too:
history does not return those, so the bridge separately looks for threads
replied to since its cursor and reads them. It scans the newest 200 messages
for such threads and reads at most 20 of them per reconnect, which is generous
for a night's sleep and stops a long absence from turning into a crawl of the
channel. Past those limits replies are genuinely missed rather than deferred,
and the server says so on stderr when it happens.

**Tapping a `slack_ask` button does nothing.** Interactivity is off on the app.
Turn it on under **Interactivity & Shortcuts** — it is a setting, not a scope,
so nothing needs reinstalling — and ask again.

**A mention in another channel is never picked up after a sleep, and stderr
says a scope is missing.** The app predates conversations outside the home
channel and has not been reinstalled since, so `users.conversations` — or
reading a page of a channel that is not the home one — is refused. The bridge
says so once and turns that catch-up off for the rest of the connection rather
than failing the calls around it: the home channel, the live stream and every
other tool go on working. Add `channels:read` and `groups:read` under **OAuth &
Permissions** — plus `channels:history` and `groups:history` for the public and
private channels it should be reading — then reinstall and restart the session:
the next connection tries again.

**`slack_history` shows user IDs instead of names.** The `users:read` scope is
missing. Add it under **OAuth & Permissions** and reinstall the app — a scope,
unlike a setting, does not reach an existing token. Everything else about the
tool works either way.

**Diagnostics.** Everything the server logs goes to stderr, prefixed
`slack-bridge:`, because stdout carries the JSON-RPC stream and a stray byte
there breaks the session. Look in the CLI's MCP log for that prefix.
