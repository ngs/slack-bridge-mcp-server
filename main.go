// Command slack-bridge-mcp-server bridges a resident Claude CLI session to a
// private Slack channel over Socket Mode, so the owner can talk to their local
// agent from anywhere.
//
// It is an MCP server on stdio: a child of the CLI, with the same lifetime as
// the session. It does not connect to Slack until the first slack_wait call,
// so it can sit in every project's .mcp.json without opening a socket.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.ngs.io/slack-bridge-mcp-server/bridge"
	"go.ngs.io/slack-bridge-mcp-server/server"
)

func main() {
	// stdout carries the JSON-RPC stream, so every diagnostic goes to stderr.
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("slack-bridge: ")

	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		_, _ = fmt.Fprintln(os.Stdout, server.VERSION)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := bridge.LoadConfig()
	if err := cfg.Validate(); err != nil {
		// Not fatal: the server still starts so slack_status can report the
		// problem to whoever is looking at the session.
		log.Printf("configuration is incomplete (%v); the bridge will fail on first use", err)
	}

	b := bridge.New(ctx, cfg, nil)
	defer func() {
		if err := b.Close(); err != nil {
			log.Printf("error releasing the bridge lock: %v", err)
		}
	}()

	if err := server.Run(ctx, server.New(b)); err != nil && ctx.Err() == nil {
		log.Printf("%v", err)
		os.Exit(1)
	}
}
