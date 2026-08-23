package bridge

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Environment variable names. These are the only configuration surface in v1;
// there is no config file and no command-line flags beyond --version.
const (
	EnvBotToken = "SLACK_BOT_TOKEN"
	EnvAppToken = "SLACK_APP_TOKEN"
	EnvChannel  = "SLACK_BRIDGE_CHANNEL"
	EnvOwner    = "SLACK_BRIDGE_OWNER"
)

// Config is the resolved environment configuration.
type Config struct {
	// BotToken is a bot user OAuth token (xoxb-...) used for the Web API.
	BotToken string
	// AppToken is an app-level token (xapp-...) with connections:write, used
	// to open the Socket Mode WebSocket.
	AppToken string
	// Channel is the Slack channel ID the bridge is bound to.
	Channel string
	// Owner is the Slack user ID whose messages are relayed. Messages from
	// anyone else in the channel are ignored.
	Owner string
	// StateDir holds state.json and the lock file. Empty means the default
	// location under the user's config directory.
	StateDir string
}

// LoadConfig reads the configuration from the process environment. It never
// fails: a Config with empty fields is returned so that the server can start
// and report the problem through slack_status. Call Validate before using the
// values.
func LoadConfig() Config {
	return Config{
		BotToken: strings.TrimSpace(os.Getenv(EnvBotToken)),
		AppToken: strings.TrimSpace(os.Getenv(EnvAppToken)),
		Channel:  strings.TrimSpace(os.Getenv(EnvChannel)),
		Owner:    strings.TrimSpace(os.Getenv(EnvOwner)),
		StateDir: strings.TrimSpace(os.Getenv("SLACK_BRIDGE_STATE_DIR")),
	}
}

// MissingVars lists the required environment variables that are unset or
// empty, in a stable order.
func (c Config) MissingVars() []string {
	var missing []string
	for _, v := range []struct {
		name  string
		value string
	}{
		{EnvBotToken, c.BotToken},
		{EnvAppToken, c.AppToken},
		{EnvChannel, c.Channel},
		{EnvOwner, c.Owner},
	} {
		if v.value == "" {
			missing = append(missing, v.name)
		}
	}
	return missing
}

// Validate reports whether the configuration is usable, naming every missing
// variable so the operator does not have to discover them one at a time.
func (c Config) Validate() error {
	missing := c.MissingVars()
	if len(missing) == 0 {
		return nil
	}
	if len(missing) == 1 {
		return fmt.Errorf("%s is not set", missing[0])
	}
	return fmt.Errorf("%s are not set", strings.Join(missing, ", "))
}

// ResolveStateDir returns the directory holding state.json and the lock file.
// The default is ~/.config/slack-bridge, honouring XDG_CONFIG_HOME. This is
// spelled out rather than delegated to os.UserConfigDir because that returns
// ~/Library/Application Support on macOS, and the bridge should keep the same
// path on every platform the owner runs a resident session on.
func (c Config) ResolveStateDir() (string, error) {
	if c.StateDir != "" {
		return c.StateDir, nil
	}
	if xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); xdg != "" {
		return filepath.Join(xdg, "slack-bridge"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine the home directory: %w", err)
	}
	return filepath.Join(home, ".config", "slack-bridge"), nil
}
