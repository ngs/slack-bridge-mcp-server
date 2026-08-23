package bridge

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigReadsTheEnvironment(t *testing.T) {
	t.Setenv(EnvBotToken, "xoxb-token")
	t.Setenv(EnvAppToken, "  xapp-token  ")
	t.Setenv(EnvChannel, "C0123456789")
	t.Setenv(EnvOwner, "U0123456789")

	cfg := LoadConfig()

	if cfg.BotToken != "xoxb-token" {
		t.Errorf("BotToken = %q, want %q", cfg.BotToken, "xoxb-token")
	}
	// Values pasted from the Slack UI routinely carry stray whitespace, which
	// would otherwise reach the API as part of the token.
	if cfg.AppToken != "xapp-token" {
		t.Errorf("AppToken = %q; want the surrounding whitespace trimmed", cfg.AppToken)
	}
	if cfg.Channel != "C0123456789" || cfg.Owner != "U0123456789" {
		t.Errorf("Channel/Owner = %q/%q, want C0123456789/U0123456789", cfg.Channel, cfg.Owner)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil for a fully populated environment", err)
	}
}

// A misconfigured bridge is the most likely first-run experience, so the error
// has to name the variables rather than saying "not configured".
func TestValidateNamesEveryMissingVariable(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		wantMissing []string
	}{
		{
			name:        "all missing",
			cfg:         Config{},
			wantMissing: []string{EnvBotToken, EnvAppToken, EnvChannel, EnvOwner},
		},
		{
			name:        "only the owner missing",
			cfg:         Config{BotToken: "b", AppToken: "a", Channel: "C1"},
			wantMissing: []string{EnvOwner},
		},
		{
			name:        "tokens present, binding missing",
			cfg:         Config{BotToken: "b", AppToken: "a"},
			wantMissing: []string{EnvChannel, EnvOwner},
		},
		{
			name: "complete",
			cfg:  Config{BotToken: "b", AppToken: "a", Channel: "C1", Owner: "U1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.MissingVars()
			if len(got) != len(tt.wantMissing) {
				t.Fatalf("MissingVars() = %v, want %v", got, tt.wantMissing)
			}
			for i := range got {
				if got[i] != tt.wantMissing[i] {
					t.Fatalf("MissingVars() = %v, want %v", got, tt.wantMissing)
				}
			}

			err := tt.cfg.Validate()
			if len(tt.wantMissing) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want an error naming the missing variables")
			}
			for _, name := range tt.wantMissing {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("Validate() = %q; want it to name %s", err, name)
				}
			}
		})
	}
}

func TestResolveStateDir(t *testing.T) {
	t.Run("explicit override wins", func(t *testing.T) {
		cfg := Config{StateDir: "/tmp/somewhere"}
		got, err := cfg.ResolveStateDir()
		if err != nil {
			t.Fatalf("ResolveStateDir() error = %v", err)
		}
		if got != "/tmp/somewhere" {
			t.Errorf("ResolveStateDir() = %q, want /tmp/somewhere", got)
		}
	})

	t.Run("XDG_CONFIG_HOME is honoured", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/xdg")
		got, err := Config{}.ResolveStateDir()
		if err != nil {
			t.Fatalf("ResolveStateDir() error = %v", err)
		}
		if want := filepath.Join("/xdg", "slack-bridge"); got != want {
			t.Errorf("ResolveStateDir() = %q, want %q", got, want)
		}
	})

	// The default must be ~/.config on every platform, not the OS-specific
	// location os.UserConfigDir would pick, so the owner's state travels with
	// their dotfiles the same way on macOS and Linux.
	t.Run("defaults to ~/.config/slack-bridge", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "")
		// os.UserHomeDir reads HOME on Unix and USERPROFILE on Windows; set
		// both so the test controls the home directory on every platform.
		t.Setenv("HOME", "/home/tester")
		t.Setenv("USERPROFILE", "/home/tester")

		got, err := Config{}.ResolveStateDir()
		if err != nil {
			t.Fatalf("ResolveStateDir() error = %v", err)
		}
		if want := filepath.Join("/home/tester", ".config", "slack-bridge"); got != want {
			t.Errorf("ResolveStateDir() = %q, want %q", got, want)
		}
	})
}
