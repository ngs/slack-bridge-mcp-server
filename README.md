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

Create a Slack app with Socket Mode on, an app-level token with
`connections:write`, the bot scopes `chat:write`, `groups:history` and
`reactions:write`, and the `message.groups` bot event. Install it, invite it to
a private channel, and set the four variables below.

## Configuration

All configuration is environment variables:

| Variable | Required | Description |
|---|---|---|
| `SLACK_BOT_TOKEN` | yes | Bot user OAuth token, `xoxb-…` |
| `SLACK_APP_TOKEN` | yes | App-level token with `connections:write`, `xapp-…` |
| `SLACK_BRIDGE_CHANNEL` | yes | Channel ID to bridge, `C…` |
| `SLACK_BRIDGE_OWNER` | yes | Your Slack user ID, `U…`. Only this user's messages are relayed. |
| `SLACK_BRIDGE_STATE_DIR` | no | Override the state directory |
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
- `bridge.lock` — an exclusive lock taken by the first `slack_wait`. A second
  concurrent bridge fails immediately rather than splitting your messages
  between two listeners.

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
needs Slack — `slack_wait`, `slack_post` or `slack_ack`, whichever comes first
— which is the lazy connect working as intended rather than a problem.
[docs/setup.md](docs/setup.md) covers the rest of the first run.

## Tools

| Tool | Arguments | Returns |
|---|---|---|
| `slack_wait` | `timeout_seconds` (optional, default 300, clamped to 5–1500) | `{"messages": [{"ts", "thread_ts"?, "user", "text"}…], "timed_out": false}`, oldest first. On timeout, `{"messages": [], "timed_out": true}`. |
| `slack_post` | `text` (required), `thread_ts` (optional) | The posted `ts` |
| `slack_ack` | `ts` (required), `emoji` (optional, default `eyes`) | Confirmation |
| `slack_status` | — | `{connected, channel, owner, last_ts, pending_backlog_count, config_error?, state_file}` |

`slack_wait` caps at 1500 seconds because Claude Code aborts a stdio MCP tool
call after 30 minutes with no response bytes; 25 minutes keeps a margin. A
`timed_out: true` result is not an error — just call it again.

## Processing indicator

Once `slack_wait` hands the agent a message, the bridge keeps the channel
posted on how long the answer is taking:

> ⏳ Working… (1m 29s)

It is posted only if the agent is still busy after the grace period, so a quick
reply leaves no trace. From then on the same message is updated in place every
interval, and it is deleted as soon as the agent replies with `slack_post` or
goes back to `slack_wait`. An ack (`slack_ack`) leaves it running — "seen, still
working" is exactly when the elapsed time is worth showing.

The whole feature is best effort: if Slack refuses any of these calls, the
failure is logged to stderr and the tools carry on unaffected. Set
`SLACK_BRIDGE_INDICATOR=off` to turn it off.

## Running a resident session

Start a session and give it a loop like this:

```
You are bridged to my Slack via the slack-bridge MCP server. Run this loop and
do not stop:

1. Call slack_wait.
2. If it returns timed_out, go back to step 1.
3. For each message: if it will take a while, call slack_ack with its ts first.
   Do what it asks, then reply with slack_post. If the message arrived in a
   thread, pass its thread_ts so the reply lands in the same thread.
4. Go back to step 1.

Keep replies short — I am reading them on a phone.
```

Then message the channel from anywhere.

## Manual smoke test

With the binary built, this drives the MCP handshake by hand and should list
the four tools. It needs no Slack credentials, since nothing connects until
`slack_wait` is called:

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
