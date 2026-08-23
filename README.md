# slack-bridge-mcp-server

Chat with your local Claude CLI session from your phone, through a private
Slack channel.

This is an [MCP](https://modelcontextprotocol.io) server that bridges a
resident Claude CLI session to one private Slack channel over Socket Mode. The
session calls a blocking `slack_wait` tool in a loop; the bridge holds the
WebSocket, catches up on anything missed while you were away, and posts the
replies back.

Messages live in Slack, so the bridge only has to remember its position in the
channel. The laptop can sleep, the session can restart, the network can drop —
when the next `slack_wait` runs, everything sent in the meantime comes back as a
backlog.

## Architecture

```
   phone / desktop Slack
            │
            │  private channel
            ▼
     ┌──────────────┐
     │    Slack     │   messages persist here
     └──────┬───────┘
            │  Socket Mode WebSocket (live)
            │  conversations.history  (catch-up)
            ▼
 ┌───────────────────────────────┐
 │  slack-bridge-mcp-server      │
 │  connection · cursor · filter │
 └───────────────┬───────────────┘
                 │  MCP over stdio
                 ▼
        ┌──────────────────┐
        │   Claude CLI     │   parent process
        │  resident session│
        └──────────────────┘
```

The bridge is a child process of the CLI with the same lifetime as the session.
No daemon, no HTTP listener, no launchd job. It does not connect to Slack until
the first tool call that needs Slack, so it can sit in every project's
`.mcp.json` without opening a socket in sessions that never use it.

The full rationale is in [docs/design.md](docs/design.md).

## Setup

**[docs/setup.md](docs/setup.md) is the full walkthrough**, from creating the
Slack app to the first message: scopes, tokens, the channel, installing the
binary, `.mcp.json`, and what to check when something does not work. There is a
[Slack app manifest](docs/slack-app-manifest.yaml) that skips most of the app
configuration.

The short version, if you have done this kind of thing before:

```sh
brew install ngs/tap/slack-bridge-mcp-server   # or: go install go.ngs.io/slack-bridge-mcp-server@latest
```

Create a Slack app with Socket Mode and interactivity on, an app-level token
with `connections:write`, the bot scopes `chat:write`, `groups:history`,
`reactions:write` and `users:read`, and the `message.groups` bot event. Install
it, invite it to a private channel, and set the four variables below.

## Configuration

All configuration is environment variables:

| Variable | Required | Description |
|---|---|---|
| `SLACK_BOT_TOKEN` | yes | Bot user OAuth token, `xoxb-…` |
| `SLACK_APP_TOKEN` | yes | App-level token with `connections:write`, `xapp-…` |
| `SLACK_BRIDGE_CHANNEL` | yes | Channel ID to bridge, `C…` |
| `SLACK_BRIDGE_OWNER` | yes | Your Slack user ID, `U…`. Only this user's messages are relayed. |
| `SLACK_BRIDGE_STATE_DIR` | no | Override the state directory |
| `SLACK_BRIDGE_AUTO_ACK` | no | `off` disables the automatic receipt reaction. Enabled by default. |
| `SLACK_BRIDGE_AUTO_ACK_EMOJI` | no | Emoji for the receipt reaction, without colons. Default `eyes`. |
| `SLACK_BRIDGE_INDICATOR` | no | `off` disables the processing indicator. Enabled by default. |
| `SLACK_BRIDGE_INDICATOR_GRACE` | no | Seconds before the indicator appears. Default 10, clamped to 3–120. |
| `SLACK_BRIDGE_INDICATOR_INTERVAL` | no | Seconds between indicator updates. Default 10, clamped to 5–60. |

An unusable value for either number falls back to the default with a note on
stderr; these settings can never keep the bridge from starting.

If any required variable is missing, the process still starts and serves MCP —
`slack_status` will tell you exactly which ones are unset. The other tools fail
with the same message.

State lives in `~/.config/slack-bridge/` (honouring `XDG_CONFIG_HOME`):

- `state.json` — `{"channels": {"<channel_id>": {"last_ts": "…"}}}`, the cursor
  into the channel, created `0600`.
- `bridge.lock` — an exclusive lock taken by the first tool call that connects
  to Slack, whichever that is. A second concurrent bridge fails immediately
  rather than splitting your messages between two listeners.

## Wiring it into Claude Code

`.mcp.json`:

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

`slack_status` answers `"connected": false` until the first tool call that
needs Slack — `slack_wait`, `slack_post`, `slack_ack`, `slack_ask`,
`slack_history` or `slack_progress`, whichever comes first — which is the lazy connect working as
intended rather than a problem. [docs/setup.md](docs/setup.md) covers the rest of the first run.

## Tools

| Tool | Arguments | Returns |
|---|---|---|
| `slack_wait` | `timeout_seconds` (optional, default 300, clamped to 5–1500) | `{"messages": [{"ts", "thread_ts"?, "user", "text"}…], "timed_out": false}`, oldest first. On timeout, `{"messages": [], "timed_out": true}`. |
| `slack_post` | `text` (required), `thread_ts` (optional) | The posted `ts` |
| `slack_ack` | `ts` (required), `emoji` (optional, default `eyes`) | Confirmation. Receipt is marked automatically, so this is for a deliberate signal beyond it. |
| `slack_ask` | `question` (required), `options` (required, 2–10), `timeout_seconds` and `thread_ts` (optional) | `{"choice_index", "choice_label", "ts", "timed_out": false}`. On timeout, `{"choice_index": -1, "timed_out": true}`. |
| `slack_history` | `limit` (optional, default 50, clamped to 1–200), `oldest`, `latest` (exclusive bounds), `thread_ts` (all optional) | `{"messages": [{"ts", "user"?, "user_name", "text", "thread_ts"?, "bot", "reply_count"?}…], "has_more"}`, oldest first, every author. A limit keeps the newest end of the window. |
| `slack_progress` | `text` (required), `thread_ts` (optional, only used if no indicator is running) | `{"ok": true, "ts"}` — the indicator message the label went on. `ts` is empty while the indicator has yet to post, and `ok` is false when the indicator is turned off. |
| `slack_status` | — | `{connected, channel, owner, last_ts, pending_backlog_count, config_error?, state_file}` |

`slack_wait` caps at 1500 seconds because Claude Code aborts a stdio MCP tool
call after 30 minutes with no response bytes; 25 minutes keeps a margin. A
`timed_out: true` result is not an error — just call it again.

## Reading the channel

`slack_wait` relays only your messages, which is the right rule for a relay and
the wrong one for "summarise what we decided up there". `slack_history` is the
other mode: ask the agent to read the channel and it gets everything, including
the colleagues, the bots and the incoming webhooks, with display names resolved
and a `reply_count` pointing at any thread worth opening (`thread_ts` reads
that thread).

It is strictly a read. It does not consume anything `slack_wait` would have
delivered, does not move the cursor, does not react, and does not disturb the
indicator — calling it changes nothing except what the model knows.

Names come from `users.info`, which needs the `users:read` scope. Without it
the tool still works and shows raw user IDs, so an app installed before this
existed keeps working until it suits you to reinstall it with the scope.

## Receipt and progress

Two signals, and they mean different things:

> **👀** — received: the message reached the session
>
> **⏳ Working… (1m 29s)** — still busy with it

The 👀 goes on every message the moment `slack_wait` hands it over, added by the
server itself rather than by the model, so the owner sees delivery at second
zero instead of whenever the model gets around to it. It is best effort and
happens off to the side: if Slack refuses the reaction, the message is still
delivered and the failure only reaches stderr.

`SLACK_BRIDGE_AUTO_ACK=off` turns it off, and `SLACK_BRIDGE_AUTO_ACK_EMOJI`
picks a different emoji (bare name, no colons). `slack_ack` stays for the
deliberate signals — done, rejected, picked up by hand — with whatever emoji
the moment calls for.

## Asking the owner a question

`slack_ask` is the bridge's version of stopping to ask. The agent posts a
question with one button per answer and blocks until you tap one:

> Ship the fix now, or hold it until the release?
>
> `[ Ship now ]` `[ Hold ]` `[ Ask me later ]`

Tapping rewrites the message with your choice and takes the buttons away, so a
question is answered exactly once; a question nobody answers is marked expired
when the timeout runs out and returns `timed_out: true`. Only the owner's clicks
count, and only one question can be outstanding at a time — a second `slack_ask`
while one is pending is refused rather than queued.

The elapsed-time indicator below stops while the question is up, since the agent
is not the one working, and starts again on your answer.

The app needs **Interactivity** turned on for the clicks to arrive; the
[manifest](docs/slack-app-manifest.yaml) sets it, and for an app installed
before this existed it is one toggle and no reinstall. See
[docs/setup.md](docs/setup.md#1-create-the-slack-app).

## Processing indicator

Once `slack_wait` hands the agent a message, the bridge keeps the channel
posted on how long the answer is taking:

> ⏳ Working… (1m 29s)

It is posted only if the agent is still busy after the grace period, so a quick
reply leaves no trace. From then on the same message is updated in place every
interval, and it is deleted as soon as the agent replies with `slack_post` or
goes back to `slack_wait`. Neither the automatic receipt nor an explicit
`slack_ack` disturbs it — "seen, still working" is exactly when the elapsed time
is worth showing.

It appears wherever the conversation is. A message sent inside a thread gets its
indicator in that thread; one sent on the channel surface gets it out there. A
`slack_ask` asked in a thread starts the next one in the same thread, and when a
batch of messages arrives at once the newest decides, since that is where the
owner spoke last.

The whole feature is best effort: if Slack refuses any of these calls, the
failure is logged to stderr and the tools carry on unaffected. Set
`SLACK_BRIDGE_INDICATOR=off` to turn it off.

## Saying what you are waiting on

A stopwatch says the agent is busy, not what with. When it starts something long
— a CI run, a release pipeline, a build — one `slack_progress` call puts the
answer next to the clock:

> ⏳ Working… (4m 10s) — release chain: waiting for CI

The agent says it once and the server does the rest: the label rides every
update from then on and goes away with the indicator, so there is no second
message to keep alive or clean up. Calling it again replaces the label, and the
next turn starts with a bare stopwatch again.

It also brings the indicator forward. The grace period exists because most
answers arrive in seconds; an agent calling `slack_progress` has just said this
one will not, so the message is posted straight away instead of waiting the
grace period out. If no indicator is running at all — long work started after a
`slack_wait` timed out, say — this starts one, in `thread_ts` if you pass it,
and it retires on the next reply or wait like any other.

With `SLACK_BRIDGE_INDICATOR=off` there is nowhere to put a label, so the call
does nothing and answers `{"ok": false}`.

## Running a resident session

Start a session and give it a loop like this:

```
You are bridged to my Slack via the slack-bridge MCP server. Run this loop and
do not stop:

1. Call slack_wait.
2. If it returns timed_out, go back to step 1.
3. For each message: do what it asks, then reply with slack_post. If the
   message arrived in a thread, pass its thread_ts so the reply lands in the
   same thread. Receipt is already marked for you, so reach for slack_ack only
   to say something an emoji says well — done, rejected, picked up by hand.
4. If you need a decision from me before you can go on, call slack_ask with the
   question and the answers to choose from, and act on what I tap.
5. If something is going to take a while — CI, a release, a long build — call
   slack_progress once with what you are waiting on, so I can see it from the
   channel.
6. Go back to step 1.

Keep replies short — I am reading them on a phone.
```

Then message the channel from anywhere.

## Manual smoke test

With the binary built, this drives the MCP handshake by hand and should list
the seven tools. It needs no Slack credentials: listing the tools calls none of
them, and it is the calls that connect.

```sh
{ printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'; sleep 1; } \
| ./slack-bridge-mcp-server 2>/dev/null
```

The `sleep` matters: without it the shell closes stdin as soon as the last
request is written, and the server shuts down on EOF before it has flushed its
replies.

Once credentials are set, calling `slack_status` the same way reports the
channel, the owner and the cursor without opening a socket.

## Security

The bridge relays only messages where the author is `SLACK_BRIDGE_OWNER` and
the channel is `SLACK_BRIDGE_CHANNEL`. Bot messages are dropped, which is what
stops the agent's own replies from being read back as new instructions, and so
are edits, deletions and join notices.

**Messages arriving over the bridge are external input reaching an agent with
local tool access.** The owner filter authenticates them as coming from your
Slack account, which is exactly as strong as that account and the phone it is
signed in on. Treat access to the channel as equivalent to terminal access on
the machine running the session, and set that session's tool permissions
accordingly.

Tokens are read from the environment and are never logged or written to the
state file.

## Development

```sh
make build        # build the binary
make test         # go test ./...
make test-coverage
make fmt
make lint         # golangci-lint
```

stdout carries the JSON-RPC stream, so every diagnostic goes to stderr. A guard
test in `stdout_guard_test.go` parses the source tree and fails if any non-test
file writes to stdout or points `log.SetOutput` anywhere but `os.Stderr`.

Releases: `make set-version VERSION=vX.Y.Z`, then push the tag. CI refuses to
publish if `server/version.go` and the tag disagree.

## License

MIT. See [LICENSE](LICENSE).
