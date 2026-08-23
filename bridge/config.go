package bridge

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Environment variable names. These are the only configuration surface in v1;
// there is no config file and no command-line flags beyond --version.
const (
	EnvBotToken = "SLACK_BOT_TOKEN"
	EnvAppToken = "SLACK_APP_TOKEN"
	EnvChannel  = "SLACK_BRIDGE_CHANNEL"
	EnvOwner    = "SLACK_BRIDGE_OWNER"

	EnvIndicator         = "SLACK_BRIDGE_INDICATOR"
	EnvIndicatorGrace    = "SLACK_BRIDGE_INDICATOR_GRACE"
	EnvIndicatorInterval = "SLACK_BRIDGE_INDICATOR_INTERVAL"
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

	// IndicatorDisabled turns the "⏳ Working…" message off. The field is
	// phrased negatively on purpose: the zero Config, which the tests and any
	// programmatic caller build, then has the indicator enabled, matching the
	// documented default.
	IndicatorDisabled bool
	// IndicatorGrace is how long the agent may take before the indicator is
	// posted at all. Zero means DefaultIndicatorGrace.
	IndicatorGrace time.Duration
	// IndicatorInterval is the gap between chat.update ticks. Zero means
	// DefaultIndicatorInterval.
	IndicatorInterval time.Duration
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

		IndicatorDisabled: indicatorDisabled(os.Getenv(EnvIndicator)),
		IndicatorGrace: durationSetting(
			EnvIndicatorGrace, DefaultIndicatorGrace, MinIndicatorGrace, MaxIndicatorGrace),
		IndicatorInterval: durationSetting(
			EnvIndicatorInterval, DefaultIndicatorInterval, MinIndicatorInterval, MaxIndicatorInterval),
	}
}

// indicatorDisabled reads the on/off switch. Only the documented "off" and the
// obvious synonyms turn the indicator off; anything else, including a typo,
// leaves the default in place, because silently losing a feature is worse than
// ignoring a value nobody meant.
func indicatorDisabled(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "off", "false", "0", "no":
		return true
	default:
		return false
	}
}

// durationSetting reads an optional number of seconds, falling back to the
// default and saying so on stderr rather than refusing to start. These settings
// are never required, so a bad value must not be able to break a session.
func durationSetting(name string, fallback, lowest, highest time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		// The value is whatever was in the environment, so it is sanitized
		// before it reaches a log line.
		log.Printf("%s = %s is not a positive number of seconds; using %s",
			name, logSafe(raw, maxLoggedValue), fallback)
		return fallback
	}

	// Clamp while the value is still a count of seconds. Converting first
	// multiplies by a billion, and a large enough number overflows
	// time.Duration into a negative value, which would then clamp to the
	// minimum — the opposite of what someone asking for a very long interval
	// meant.
	if seconds <= int(lowest/time.Second) {
		return lowest
	}
	if seconds >= int(highest/time.Second) {
		return highest
	}
	return time.Duration(seconds) * time.Second
}

// indicatorTimings resolves the durations actually used, so a Config built in
// code rather than from the environment still gets the documented defaults
// instead of a zero grace period that would post immediately.
func (c Config) indicatorTimings() (grace, interval time.Duration) {
	grace, interval = c.IndicatorGrace, c.IndicatorInterval
	if grace <= 0 {
		grace = DefaultIndicatorGrace
	}
	if interval <= 0 {
		interval = DefaultIndicatorInterval
	}
	return grace, interval
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
