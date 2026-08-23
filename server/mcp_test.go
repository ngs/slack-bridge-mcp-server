package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.ngs.io/slack-bridge-mcp-server/bridge"
)

// connect wires a client to the server over the SDK's in-memory transport, so
// the tests exercise the real MCP handshake and tool dispatch without a
// subprocess or a socket.
func connect(t *testing.T, b *bridge.Bridge) *mcp.ClientSession {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()

	srv := New(b)
	serverSession, err := srv.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("connecting the server: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("connecting the client: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return session
}

// unconfiguredBridge has no Slack credentials, which is exactly the state
// slack_status has to survive.
func unconfiguredBridge(t *testing.T) *bridge.Bridge {
	t.Helper()
	b := bridge.New(context.Background(), bridge.Config{StateDir: t.TempDir()}, nil)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// The tool names are the server's contract with the resident session's prompt
// loop; renaming one silently breaks every .mcp.json in the wild.
func TestServerExposesTheBridgeTools(t *testing.T) {
	session := connect(t, unconfiguredBridge(t))

	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	got := make(map[string]*mcp.Tool, len(result.Tools))
	for _, tool := range result.Tools {
		got[tool.Name] = tool
	}

	want := []string{"slack_wait", "slack_post", "slack_ack", "slack_ask", "slack_history", "slack_progress", "slack_status"}
	if len(got) != len(want) {
		t.Errorf("ListTools() returned %d tools, want %d", len(got), len(want))
	}
	for _, name := range want {
		tool, ok := got[name]
		if !ok {
			t.Errorf("ListTools() is missing %s", name)
			continue
		}
		if tool.Description == "" {
			t.Errorf("%s has no description; the model has nothing to go on", name)
		}
		if tool.InputSchema == nil {
			t.Errorf("%s has no input schema", name)
		}
	}
}

// The read-only hint is something a client may act on without asking, so it
// has to be true. slack_wait reacts to what it delivers and starts the
// indicator; slack_history really does only read.
func TestOnlyTheReadOnlyToolsSayTheyAre(t *testing.T) {
	session := connect(t, unconfiguredBridge(t))

	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	readOnly := map[string]bool{"slack_history": true, "slack_status": true}
	for _, tool := range result.Tools {
		if tool.Annotations == nil {
			t.Errorf("%s has no annotations", tool.Name)
			continue
		}
		if got := tool.Annotations.ReadOnlyHint; got != readOnly[tool.Name] {
			t.Errorf("%s ReadOnlyHint = %v, want %v", tool.Name, got, readOnly[tool.Name])
		}
	}
}

// slack_wait's timeout is the one argument a model is likely to guess at, so
// the schema has to advertise it as optional with the documented range.
func TestWaitToolAdvertisesAnOptionalTimeout(t *testing.T) {
	session := connect(t, unconfiguredBridge(t))

	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	for _, tool := range result.Tools {
		if tool.Name != "slack_wait" {
			continue
		}

		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatalf("marshalling the schema: %v", err)
		}

		var decoded struct {
			Properties map[string]any `json:"properties"`
			Required   []string       `json:"required"`
		}
		if err := json.Unmarshal(schema, &decoded); err != nil {
			t.Fatalf("decoding the schema: %v", err)
		}

		if _, ok := decoded.Properties["timeout_seconds"]; !ok {
			t.Errorf("slack_wait schema has no timeout_seconds property: %s", schema)
		}
		if len(decoded.Required) != 0 {
			t.Errorf("slack_wait requires %v; every argument should be optional", decoded.Required)
		}
		return
	}
	t.Fatal("slack_wait was not listed")
}

// The label is the whole point of slack_progress, so the schema has to say it
// is required — a call without one has nothing to show.
func TestProgressToolRequiresItsText(t *testing.T) {
	session := connect(t, unconfiguredBridge(t))

	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}

	for _, tool := range result.Tools {
		if tool.Name != "slack_progress" {
			continue
		}

		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatalf("marshalling the schema: %v", err)
		}

		var decoded struct {
			Properties map[string]any `json:"properties"`
			Required   []string       `json:"required"`
		}
		if err := json.Unmarshal(schema, &decoded); err != nil {
			t.Fatalf("decoding the schema: %v", err)
		}

		if _, ok := decoded.Properties["thread_ts"]; !ok {
			t.Errorf("slack_progress schema has no thread_ts property: %s", schema)
		}
		if len(decoded.Required) != 1 || decoded.Required[0] != "text" {
			t.Errorf("slack_progress requires %v, want text and nothing else", decoded.Required)
		}
		return
	}
	t.Fatal("slack_progress was not listed")
}

// slack_status is the diagnostic path: it must answer over MCP even though no
// Slack credentials exist, and it must say which variables are missing.
func TestStatusToolWorksWithoutConfiguration(t *testing.T) {
	session := connect(t, unconfiguredBridge(t))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "slack_status"})
	if err != nil {
		t.Fatalf("CallTool(slack_status) error = %v", err)
	}
	if result.IsError {
		t.Fatalf("slack_status reported an error: %+v", result.Content)
	}

	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatalf("marshalling the result: %v", err)
	}

	var status bridge.Status
	if err := json.Unmarshal(encoded, &status); err != nil {
		t.Fatalf("decoding the result: %v", err)
	}

	if status.Connected {
		t.Error("slack_status reports connected on a bridge that never opened a socket")
	}
	if status.ConfigError == "" {
		t.Errorf("slack_status config_error is empty; want the missing variables named. Got %s", encoded)
	}
	for _, name := range []string{bridge.EnvBotToken, bridge.EnvAppToken, bridge.EnvChannel, bridge.EnvOwner} {
		if !strings.Contains(status.ConfigError, name) {
			t.Errorf("config_error = %q; want it to name %s", status.ConfigError, name)
		}
	}
}

// A misconfigured slack_wait must come back as a tool error the model can read
// and act on, not as a protocol-level failure that aborts the call.
func TestWaitReportsMissingConfigurationAsAToolError(t *testing.T) {
	session := connect(t, unconfiguredBridge(t))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "slack_wait",
		Arguments: map[string]any{"timeout_seconds": 5},
	})
	if err != nil {
		t.Fatalf("CallTool(slack_wait) error = %v; want the failure inside the result", err)
	}
	if !result.IsError {
		t.Fatal("slack_wait succeeded on an unconfigured bridge, want IsError")
	}

	text := renderContent(result.Content)
	if !strings.Contains(text, bridge.EnvBotToken) {
		t.Errorf("slack_wait error = %q; want it to name %s", text, bridge.EnvBotToken)
	}
}

func TestServerReportsItsVersion(t *testing.T) {
	session := connect(t, unconfiguredBridge(t))

	info := session.InitializeResult().ServerInfo
	if info.Name != ServerName {
		t.Errorf("serverInfo.name = %q, want %q", info.Name, ServerName)
	}
	if info.Version != VERSION {
		t.Errorf("serverInfo.version = %q, want %q", info.Version, VERSION)
	}
}

func renderContent(content []mcp.Content) string {
	var out string
	for _, c := range content {
		if text, ok := c.(*mcp.TextContent); ok {
			out += text.Text
		}
	}
	return out
}
