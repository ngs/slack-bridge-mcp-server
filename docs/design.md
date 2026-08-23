# Design

## Background

A resident Claude CLI session on a desktop machine is useful, but it is tied to
that machine's terminal. The owner is often somewhere else with only a phone.

Slack already solves the "somewhere else with only a phone" half: it has good
mobile clients, it stores messages durably, and a private channel is a natural
place for a one-to-one conversation. What is missing is a way for the local
agent session to read from and write to that channel.

This project is that piece and nothing more. It is an MCP server the CLI loads
like any other tool provider. The agent calls a blocking `slack_wait` in a
loop; the bridge holds the WebSocket, catches up on anything missed, and hands
messages over. Replies go back through `slack_post`.

Concretely, a resident session runs a prompt along the lines of *"call
`slack_wait`; when a message comes back, do what it asks and reply with
`slack_post`; then call `slack_wait` again"*, and the owner gets a working chat
with their own machine.

## Architecture

```
   phone / desktop Slack
            │
            │  private channel #agent
            ▼
     ┌──────────────┐
     │    Slack     │   messages persist here; this is the source of truth
     └──────┬───────┘
            │  Socket Mode WebSocket (live)
            │  conversations.history  (catch-up)
            ▼
 ┌───────────────────────────────┐
 │  slack-bridge-mcp-server      │   this project
 │                               │
 │  bridge/  connection, cursor, │
 │           filtering, merge    │
 │  server/  MCP tool surface    │
 └───────────────┬───────────────┘
                 │  MCP over stdio (JSON-RPC on stdin/stdout)
                 ▼
        ┌──────────────────┐
        │   Claude CLI     │   parent process; owns the session
        │  resident session│
        └──────────────────┘
```

The bridge is a child process of the CLI with exactly the session's lifetime.
There is no daemon, no HTTP listener, no launchd job, and no state shared
between sessions beyond a single cursor file.

## Fixed decisions

### 1. A stdio MCP server, not a service

The bridge runs as a child process of the Claude CLI and speaks MCP over
stdin/stdout. It starts when the session starts and dies when the session dies.

The alternative — a long-lived daemon the session talks to over HTTP — would
need its own lifecycle management, its own port, its own authentication between
the CLI and the daemon, and a story for what happens when two sessions connect
to it. A child process gets all of that for free from the operating system.

The cost is that the Slack connection does not survive between sessions.
Decision 3 makes that cost small.

### 2. Lazy connect

Nothing touches the network at process startup. The Socket Mode connection is
opened on the first `slack_wait` call.

This is what lets the server sit in every project's `.mcp.json` unconditionally.
Most sessions never call `slack_wait`, and those sessions never open a socket,
never authenticate, and never take the single-instance lock. A server that
connected eagerly would either have to be added and removed by hand per
session, or would open a Slack WebSocket every time the owner opened a terminal.

`slack_status` deliberately does not connect either, so it stays answerable when
the configuration is broken — which is precisely when someone reaches for it.

### 3. Catch up over Slack's own history

Slack stores messages. That makes it the durable queue, and the bridge only has
to remember a position in it.

On every connect and reconnect, the bridge calls `conversations.history` with
`oldest` set to the persisted cursor, collects anything it has not handed over
yet, and only then waits for live events. The cursor is persisted to disk, so it
survives the process exiting.

This is what makes the whole design tolerable in practice. The laptop sleeps,
the session is restarted, the WebSocket drops on a network change — in every
case the messages sent meanwhile are still in Slack, and the next `slack_wait`
returns them as a backlog array rather than the owner discovering hours later
that nobody was listening.

A message can arrive twice: once over the WebSocket around the moment of a
reconnect, and again in the history page fetched for that same reconnect. The
merge step deduplicates by timestamp, so how that race resolves does not matter.

The first run against a channel is the one case where catch-up is skipped. With
no cursor, the bridge records the newest existing message and starts from there,
rather than replaying the channel's entire history into the model's context.

#### Thread replies need a second pass

`conversations.history` returns the channel surface and nothing else: a reply
inside a thread is invisible to it. That was a real bug rather than a
theoretical one — with a thread-first conversation habit, a reply typed while
the laptop slept was never delivered at all, and `slack_wait` sat blocked until
the owner said something new on the surface.

So catch-up runs a second pass. It fetches one page of recent surface messages
**without** the cursor bound, because a thread whose parent is a week old can
still have a reply from five minutes ago, and looks at `latest_reply`, which
Slack puts on every threaded parent. A parent whose newest reply is later than
the cursor has been talked in since the bridge last looked, so the bridge reads
that thread with `conversations.replies` from the cursor forward. Recovered
replies go through the same owner filter and the same merge as everything else,
keeping their `thread_ts` so the agent answers where the owner is talking.

Two limits keep this from growing into a crawl of the channel:

- Only the newest 200 surface messages are scanned for threads. A reply added
  to a thread that has since been pushed past that window is not recovered.
- At most 20 threads are read per catch-up, newest-first, which is where a
  conversation the owner is actually having will be.

Both limits lose messages when they bite, and it is worth being exact about
that rather than comfortable: the cursor advances to the newest message that
*was* recovered, so replies in a skipped thread are older than the cursor from
then on and no later pass goes back for them. The server logs the skip count
saying as much. The alternative — a cursor per thread, or refusing to advance
past any thread not examined — buys completeness at the price of a single
recoverable position in the channel, which is the thing that makes sleep
recovery simple enough to trust. Within a thread that *is* read there is no
such gap: it is paged to the end before the cursor moves, up to ten thousand
replies in one thread, and reaching even that is logged.

The pass is skipped entirely when there is no cursor, for the same reason the
first run does not replay the channel.

A failure to read one thread is answered by what kind of failure it is. A
thread that is not there — a deleted parent, or the bot no longer in the
channel — will not be there next time either, so it is logged and skipped;
failing forever on it would wedge every later message behind it. Anything else,
a rate limit or a network blip, fails the whole catch-up on purpose: the cursor
stays where it is and the next attempt asks for the same window again, rather
than stepping over replies nobody ever read.

One consequence is worth stating: the cursor can now land on a thread reply
whose timestamp is newer than every surface message. That is correct. The
cursor tracks what the agent has been handed, not where the channel surface has
got to.

### 4. Never spawn AI from the bridge

The bridge transports messages. It does not run `claude -p`, does not shell out,
and does not start subprocesses of any kind.

The agent is the parent process, and it decides what to do with a message. A
bridge that spawned its own inference would be a second, invisible agent with no
session context, no shared memory of the conversation, and its own token spend
that the owner never authorised.

### 5. Owner filter

Only messages where `user` equals `SLACK_BRIDGE_OWNER` and `channel` equals
`SLACK_BRIDGE_CHANNEL` are relayed. Everything else is dropped:

- **Bot messages**, identified by a non-empty `bot_id`. This is what stops the
  bridge from feeding the agent's own `slack_post` output back to it as new
  input, which would otherwise loop.
- **Subtypes** other than none and `thread_broadcast`. `message_changed`,
  `message_deleted`, `channel_join`, `file_share` and the rest are either not
  new text or not the owner speaking. A plain message and a plain thread reply
  both carry no subtype at all, so both pass.
- **Empty bodies**, which have nothing to transport.

Thread replies are relayed and carry their `thread_ts`, so the agent can reply
into the same thread.

The same filter is applied to history results and to live events, from one
function, so a message cannot be relayed live but dropped on catch-up.

## Tools

| Tool | Arguments | Behaviour |
|---|---|---|
| `slack_wait` | `timeout_seconds` (optional, default 300, clamped to 5–1500) | Blocks. The first call connects and catches up. Returns as soon as at least one message is available; a catch-up backlog comes back immediately as an array. On timeout: `{"messages": [], "timed_out": true}`. Otherwise `{"messages": [{"ts", "thread_ts"?, "user", "text"}, …], "timed_out": false}`, oldest first. |
| `slack_post` | `text` (required), `thread_ts` (optional) | `chat.postMessage` to the bound channel. Returns the posted `ts`. |
| `slack_ack` | `ts` (required), `emoji` (optional, default `eyes`) | `reactions.add` on that message. Receipt is already marked automatically for everything `slack_wait` returns, so this is for a deliberate signal beyond it. An emoji already present counts as success. |
| `slack_ask` | `question` (required), `options` (required, 2–10), `timeout_seconds` and `thread_ts` (optional) | Posts a question with one button per option and blocks for a click. Returns `{"choice_index", "choice_label", "ts", "timed_out": false}`, or `{"choice_index": -1, "timed_out": true}`. The message is rewritten without its buttons either way. |
| `slack_history` | `limit` (optional, default 50, clamped to 1–200), `oldest`, `latest` (exclusive, as Slack treats them), `thread_ts` (all optional) | `conversations.history`, or `conversations.replies` when `thread_ts` is given. Returns every author, oldest first, with names resolved through `users.info`, keeping the newest `limit` of the window in both modes. Read-only: no cursor movement, no reactions, no indicator. |
| `slack_status` | — | `{connected, channel, owner, last_ts, pending_backlog_count, config_error?, state_file}`. Never connects. |

### Why slack_history ignores the owner filter

Everything else in the bridge exists to relay one person. `slack_history` is
the exception, and deliberately so: the owner asks for it by name, to read a
discussion they had with other people about the agent's work. Filtering that
down to the owner's own lines would leave the model reading half a conversation.

The safety story is different because the direction is different. Relayed
messages are instructions the agent acts on, which is why they are restricted
to one authenticated user. History is material the owner asked the agent to
read, and the server instructions say so in as many words: it is text to read,
not instructions to follow. Nothing in it reaches the agent unless the owner
asked for it.

Being read-only is what keeps the two apart. The tool cannot move the cursor,
consume a pending message, react, or touch the indicator, so no amount of
reading changes what the relay will deliver next.

### Clicks travel apart from messages

A message that cannot be queued live is not lost: the overflow becomes a
reconnect-shaped event and the bridge re-reads the window from
`conversations.history`. That safety net does not exist for a `slack_ask`
button click — no history call returns it — so a dropped click is an answer the
owner gave that the agent never sees, and the question times out as if they had
ignored it.

Clicks therefore have a queue of their own, small and read by both the wait and
the ask loops. A backlog of messages, which is the one situation where the
event queue fills, cannot take the space a click needs.

### The receipt reaction

When `slack_wait` hands messages over, the server reacts to each of them
(`eyes` by default) on a goroutine of its own. It is the server rather than the
model that does it, because the value is entirely in the timing: a receipt that
waits for the model to decide to send one is no longer a receipt.

That makes it best effort by construction. It runs on the bridge's own context
so the tool call returns immediately, and any failure is logged and dropped —
losing a courtesy reaction must never cost the owner a message. `already_reacted`
is success, since the message is marked either way.

`SLACK_BRIDGE_AUTO_ACK=off` disables it; `SLACK_BRIDGE_AUTO_ACK_EMOJI` changes
the emoji.

Out-of-range timeouts are clamped rather than rejected: a caller asking for an
hour gets the longest safe poll instead of an error it has to learn to avoid.

## Why the timeout caps at 1500 seconds

Claude Code aborts a stdio MCP tool call once no response bytes have arrived
for 30 minutes. A blocking long-poll produces no bytes at all while it waits,
so an unbounded `slack_wait` would eventually trip that idle abort and surface
as a tool failure rather than a clean empty return.

1500 seconds (25 minutes) keeps a five-minute margin under that window. The
default of 300 seconds is a comfortable poll: long enough that a session sits
quietly rather than spinning, short enough that a timeout is cheap to retry.

When `slack_wait` returns `timed_out: true`, the correct response is simply to
call it again. The timeout is a keepalive, not an error.

## Security model

The bridge is a private, single-owner tool, and its safety rests on three
things:

**A private channel.** The bot is invited to one private channel. It has no
scopes for anything else, and `groups:history` only reaches private channels it
is a member of.

**The owner filter.** Even inside that channel, only one Slack user ID is
relayed. Someone else invited to the channel can read the conversation but
cannot drive the agent — and that includes tapping a `slack_ask` button, which
carries the clicker's user ID and is checked against the owner exactly as a
message is.

**Credentials stay in the environment.** Tokens are read from the process
environment and never written to the state file or logged. The state file holds
only a timestamp cursor per channel, and is created `0600`.

### The external-input caveat

This is the part that deserves care. Messages arriving over the bridge are
**external input that reaches an agent with local tool access**. The owner filter
means the input is authenticated as coming from the owner's Slack account, but
that is exactly as strong as that Slack account and the owner's phone.

Anyone who can post as the owner in that channel can direct the local agent.
Treat access to the channel as equivalent to terminal access on the machine
running the session, and configure the session's own tool permissions
accordingly. The bridge itself makes no attempt to sanitise or constrain what a
message asks for; that is the agent's permission model's job, not the
transport's.

### Single instance

The first `slack_wait` takes an exclusive `flock` on a lock file next to the
state file. A second concurrent bridge fails immediately with a clear error
instead of starting.

Two bridges on one channel would each receive some fraction of the live events
and would race on the cursor, so the owner would see the agent answer some
messages and silently ignore others. Failing loudly is much better than that.
The lock is held by the operating system and released when the process exits,
including on a crash, so a stale lock file never blocks the next session.

## MCP SDK choice

The official Go SDK, `github.com/modelcontextprotocol/go-sdk`, at v1.7.0.

It provides `mcp.StdioTransport` directly, and its generic `mcp.AddTool` infers
both the input and the output JSON Schema from Go types, which keeps the tool
definitions in `server/mcp.go` down to a struct and a function each. It also
ships an in-memory transport, so the server tests run a real MCP handshake and
real tool dispatch without a subprocess or a socket.

Neither `github.com/mark3labs/mcp-go` nor a hand-rolled JSON-RPC layer was
needed. The one constraint the SDK imposes is a Go 1.25 minimum, which is why
`go.mod` says `go 1.25.0` rather than matching the sibling project's 1.24.

## Non-goals for v1

- **Threads as conversation context.** Thread replies are relayed and carry
  `thread_ts` so the agent can reply in place, but the bridge does not fetch a
  thread's history or present a thread as a distinct conversation.
- **Multiple channels.** One channel per process. The state file is already
  keyed by channel so this can be added without a migration.
- **Direct messages.** Only a channel, which keeps the required scopes to
  `groups:history` and the event subscription to `message.groups`.
- **Attachments and files.** Text only. Messages whose body is empty because
  the content is a file are dropped rather than relayed as an empty string.
- **Block Kit and interactive components.** Replies are plain mrkdwn. No
  buttons, no modals, no slash commands, and so no interactivity payloads to
  handle.
- **Editing and deletion.** `message_changed` and `message_deleted` are ignored;
  an edited message is not re-delivered.
