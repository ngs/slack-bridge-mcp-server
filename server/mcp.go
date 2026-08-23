// Package server wires the bridge onto the Model Context Protocol.
//
// The server speaks MCP over stdio as a child process of the Claude CLI, so it
// shares the session's lifetime exactly. Nothing but the protocol may be
// written to stdout; every diagnostic goes to stderr through the log package,
// an invariant the stdout guard test in the repository root enforces at the
// source level.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.ngs.io/slack-bridge-mcp-server/bridge"
)

// ServerName identifies this implementation in the MCP initialize handshake.
const ServerName = "slack-bridge-mcp-server"

// instructions tell the model how the tools fit together, since the intended
// use is an unusual one: a long-running poll loop rather than a request per
// user turn.
const instructions = `Bridges this session to a private Slack channel so the owner can talk to you from their phone.

Call slack_wait to block until the owner sends a message; it returns immediately with
anything that arrived while you were busy or the session was down. Reply with slack_post.
Use slack_ack to acknowledge a message you have started working on but cannot answer yet.
Use slack_ask to ask the owner a multiple-choice question and block for the answer, the way
you would ask in the terminal when you need a decision before you can go on.
When slack_wait returns timed_out, simply call it again to keep the conversation open.`

// WaitArgs is the argument set for slack_wait.
type WaitArgs struct {
	TimeoutSeconds int `json:"timeout_seconds,omitempty" jsonschema:"how long to block before returning empty, in seconds; defaults to 300 and is clamped to 5-1500"`
}

// PostArgs is the argument set for slack_post.
type PostArgs struct {
	Text     string `json:"text" jsonschema:"the message body to send, as Slack mrkdwn"`
	ThreadTS string `json:"thread_ts,omitempty" jsonschema:"reply inside this thread instead of the channel; pass the thread_ts of a message from slack_wait"`
}

// AckArgs is the argument set for slack_ack.
type AckArgs struct {
	TS    string `json:"ts" jsonschema:"the ts of the message to react to, as returned by slack_wait"`
	Emoji string `json:"emoji,omitempty" jsonschema:"emoji name without colons; defaults to eyes"`
}

// AskArgs is the argument set for slack_ask.
type AskArgs struct {
	Question       string   `json:"question" jsonschema:"the question to ask, as Slack mrkdwn"`
	Options        []string `json:"options" jsonschema:"the answers to offer as buttons, between 2 and 10; labels longer than 75 characters are shortened"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty" jsonschema:"how long to wait for an answer, in seconds; defaults to 300 and is clamped to 5-1500"`
	ThreadTS       string   `json:"thread_ts,omitempty" jsonschema:"ask inside this thread instead of the channel"`
}

// StatusArgs is empty: slack_status takes no arguments.
type StatusArgs struct{}

// PostResult reports where the message landed.
type PostResult struct {
	TS      string `json:"ts"`
	Channel string `json:"channel"`
}

// AckResult confirms the reaction.
type AckResult struct {
	OK    bool   `json:"ok"`
	TS    string `json:"ts"`
	Emoji string `json:"emoji"`
}

// New builds the MCP server and registers the five bridge tools.
func New(b *bridge.Bridge) *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    ServerName,
		Title:   "Slack Bridge",
		Version: VERSION,
	}, &mcp.ServerOptions{Instructions: instructions})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "slack_wait",
		Title:       "Wait for a Slack message",
		Description: "Block until the owner sends a message to the bridged Slack channel, or the timeout expires. Returns any messages missed while the session was down.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: boolPtr(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args WaitArgs) (*mcp.CallToolResult, bridge.WaitResult, error) {
		result, err := b.Wait(ctx, bridge.ClampTimeout(args.TimeoutSeconds))
		if err != nil {
			return nil, bridge.WaitResult{}, err
		}
		if result.Messages == nil {
			result.Messages = []bridge.Message{}
		}
		return nil, result, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "slack_post",
		Title:       "Post to Slack",
		Description: "Send a message to the bridged Slack channel, optionally as a reply inside a thread.",
		Annotations: &mcp.ToolAnnotations{OpenWorldHint: boolPtr(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args PostArgs) (*mcp.CallToolResult, PostResult, error) {
		ts, err := b.Post(ctx, args.Text, args.ThreadTS)
		if err != nil {
			return nil, PostResult{}, err
		}
		return nil, PostResult{TS: ts, Channel: b.Status().Channel}, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "slack_ack",
		Title:       "Acknowledge a Slack message",
		Description: "Add an emoji reaction to a message, to show the owner it has been seen without posting a reply.",
		Annotations: &mcp.ToolAnnotations{OpenWorldHint: boolPtr(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args AckArgs) (*mcp.CallToolResult, AckResult, error) {
		emoji := args.Emoji
		if emoji == "" {
			emoji = "eyes"
		}
		if err := b.React(ctx, args.TS, emoji); err != nil {
			return nil, AckResult{}, err
		}
		return nil, AckResult{OK: true, TS: args.TS, Emoji: emoji}, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "slack_ask",
		Title:       "Ask the owner a question",
		Description: "Post a multiple-choice question to the bridged Slack channel and block until the owner taps an answer, or the timeout expires. Returns the chosen option.",
		Annotations: &mcp.ToolAnnotations{OpenWorldHint: boolPtr(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args AskArgs) (*mcp.CallToolResult, bridge.AskResult, error) {
		result, err := b.Ask(ctx, args.Question, args.Options, bridge.ClampTimeout(args.TimeoutSeconds), args.ThreadTS)
		if err != nil {
			return nil, bridge.AskResult{}, err
		}
		return nil, result, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "slack_status",
		Title:       "Slack bridge status",
		Description: "Report whether the bridge is connected, which channel and owner it is bound to, and any missing configuration. Does not connect to Slack.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(_ context.Context, _ *mcp.CallToolRequest, _ StatusArgs) (*mcp.CallToolResult, bridge.Status, error) {
		return nil, b.Status(), nil
	})

	return srv
}

// Run serves MCP on stdio until the client disconnects or ctx is cancelled.
//
// The client closing stdin is how a normal shutdown looks from here: the CLI
// exited and took its child with it. That surfaces as an EOF, which is
// reported as a clean return rather than a failure so the process does not
// exit non-zero every time the session ends.
func Run(ctx context.Context, srv *mcp.Server) error {
	err := srv.Run(ctx, &mcp.StdioTransport{})
	switch {
	case err == nil, errors.Is(err, io.EOF), errors.Is(err, mcp.ErrConnectionClosed):
		return nil
	default:
		return fmt.Errorf("serving MCP over stdio: %w", err)
	}
}

func boolPtr(b bool) *bool { return &b }
