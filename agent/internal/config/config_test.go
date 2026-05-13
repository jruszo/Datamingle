package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFileAppliesDefaultsAndParsesConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.yaml")
	content := `
# Datamingle Agent
datamingle_url: "https://datamingle.example.com"
agent_name: prod-db-agent-01
data_dir: /tmp/datamingle-agent/data # inline comment
log_dir: /tmp/datamingle-agent/log
runtime_dir: /tmp/datamingle-agent/run
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.DatamingleURL != "https://datamingle.example.com" {
		t.Fatalf("unexpected URL %q", cfg.DatamingleURL)
	}
	if cfg.APIKeyEnv != "DATAMINGLE_AGENT_API_KEY" {
		t.Fatalf("expected default api key env, got %q", cfg.APIKeyEnv)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config: %v", err)
	}
}

func TestValidateRejectsMissingURL(t *testing.T) {
	cfg := Default()
	cfg.DatamingleURL = ""

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateRejectsRelativePaths(t *testing.T) {
	cfg := Default()
	cfg.DatamingleURL = "https://datamingle.example.com"
	cfg.DataDir = "relative"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}
