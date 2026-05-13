package config

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const DefaultConfigPath = "/etc/datamingle-agent/agent.yaml"

// Config is the durable local agent configuration. Secrets are read through
// APIKeyEnv and are never written to this file.
type Config struct {
	DatamingleURL string
	APIKeyEnv     string
	AgentName     string
	DataDir       string
	LogDir        string
	RuntimeDir    string
}

func Default() Config {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "datamingle-agent"
	}

	return Config{
		APIKeyEnv:  "DATAMINGLE_AGENT_API_KEY",
		AgentName:  hostname,
		DataDir:    "/var/lib/datamingle-agent",
		LogDir:     "/var/log/datamingle-agent",
		RuntimeDir: "/run/datamingle-agent",
	}
}

// LoadFile loads the agent YAML config. The V1 config intentionally supports a
// simple key:value subset so the initial agent has no third-party dependency.
func LoadFile(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()

	cfg := Default()
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return Config{}, fmt.Errorf("%s:%d: expected key: value", path, lineNumber)
		}
		key = strings.TrimSpace(key)
		value = stripInlineComment(strings.TrimSpace(value))
		value = strings.Trim(strings.TrimSpace(value), `"'`)

		switch key {
		case "datamingle_url":
			cfg.DatamingleURL = value
		case "api_key_env":
			cfg.APIKeyEnv = value
		case "agent_name":
			cfg.AgentName = value
		case "data_dir":
			cfg.DataDir = value
		case "log_dir":
			cfg.LogDir = value
		case "runtime_dir":
			cfg.RuntimeDir = value
		default:
			return Config{}, fmt.Errorf("%s:%d: unknown key %q", path, lineNumber, key)
		}
	}
	if err := scanner.Err(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func stripInlineComment(value string) string {
	quote := rune(0)
	for i, r := range value {
		switch r {
		case '\'', '"':
			if quote == 0 {
				quote = r
			} else if quote == r {
				quote = 0
			}
		case '#':
			if quote == 0 && (i == 0 || value[i-1] == ' ' || value[i-1] == '\t') {
				return strings.TrimSpace(value[:i])
			}
		}
	}
	return value
}

func (cfg Config) Validate() error {
	if strings.TrimSpace(cfg.DatamingleURL) == "" {
		return fmt.Errorf("datamingle_url is required")
	}
	parsed, err := url.Parse(cfg.DatamingleURL)
	if err != nil {
		return fmt.Errorf("datamingle_url is invalid: %w", err)
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return fmt.Errorf("datamingle_url must use http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("datamingle_url must include a host")
	}
	if strings.TrimSpace(cfg.APIKeyEnv) == "" {
		return fmt.Errorf("api_key_env is required")
	}
	if strings.TrimSpace(cfg.AgentName) == "" {
		return fmt.Errorf("agent_name is required")
	}
	for name, path := range map[string]string{
		"data_dir":    cfg.DataDir,
		"log_dir":     cfg.LogDir,
		"runtime_dir": cfg.RuntimeDir,
	} {
		if !filepath.IsAbs(path) {
			return fmt.Errorf("%s must be an absolute path", name)
		}
	}
	return nil
}
