package monitoring

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/jruszo/datamingle/agent/internal/modules"
	"github.com/jruszo/datamingle/agent/internal/tools"
)

type Module struct {
	mu             sync.RWMutex
	dataDir        string
	apiKeyEnv      string
	httpClient     *http.Client
	cancel         context.CancelFunc
	process        *exec.Cmd
	enabled        bool
	revision       int64
	status         string
	message        string
	lastScrapeAt   time.Time
	lastWriteAt    time.Time
	lastWriteCount int
}

func New(dataDir, apiKeyEnv string) *Module {
	return &Module{
		dataDir:    dataDir,
		apiKeyEnv:  apiKeyEnv,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		status:     "disabled",
		message:    "node monitoring is disabled",
	}
}

func (m *Module) Name() string {
	return "node_monitoring"
}

func (m *Module) Capabilities() []string {
	return []string{"node.metrics"}
}

func (m *Module) ApplyConfig(ctx context.Context, cfg modules.Config) error {
	m.stopActive(context.Background())

	m.mu.Lock()
	m.enabled = cfg.Enabled
	m.revision = cfg.Revision
	m.status = "disabled"
	m.message = "node monitoring is disabled"
	m.lastScrapeAt = time.Time{}
	m.lastWriteAt = time.Time{}
	m.lastWriteCount = 0
	m.mu.Unlock()

	if !cfg.Enabled {
		return nil
	}

	parsed, err := parseConfig(cfg.Raw)
	if err != nil {
		m.setHealth("degraded", err.Error())
		return nil
	}
	if parsed.RemoteWriteURL == "" {
		m.setHealth("degraded", "remote write URL is not configured")
		return nil
	}
	if parsed.NodeExporter.Artifact.DownloadURL == "" {
		m.setHealth("degraded", "node_exporter artifact is not configured")
		return nil
	}

	binaryPath, err := tools.EnsureArtifact(
		ctx,
		filepath.Join(m.dataDir, "tools"),
		parsed.NodeExporter.Artifact,
		m.httpClient,
	)
	if err != nil {
		m.setHealth("degraded", fmt.Sprintf("node_exporter artifact unavailable: %v", err))
		return nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.Command(
		binaryPath,
		"--web.listen-address="+parsed.NodeExporter.ListenAddress,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		m.setHealth("degraded", fmt.Sprintf("node_exporter failed to start: %v", err))
		return nil
	}

	m.mu.Lock()
	m.cancel = cancel
	m.process = cmd
	m.status = "online"
	m.message = "node_exporter running"
	m.mu.Unlock()

	go m.waitForNodeExporter(cmd)
	go m.scrapeLoop(runCtx, parsed)
	return nil
}

func (m *Module) Health(ctx context.Context) modules.Health {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return modules.Health{
		Module:    m.Name(),
		Status:    m.status,
		Message:   m.message,
		UpdatedAt: time.Now().UTC(),
		Details: map[string]any{
			"enabled":          m.enabled,
			"revision":         m.revision,
			"last_scrape_at":   m.lastScrapeAt,
			"last_write_at":    m.lastWriteAt,
			"last_write_count": m.lastWriteCount,
		},
	}
}

func (m *Module) Stop(ctx context.Context) error {
	m.stopActive(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = false
	m.status = "disabled"
	m.message = "node monitoring is disabled"
	return nil
}

func (m *Module) scrapeLoop(ctx context.Context, cfg config) {
	interval := cfg.ScrapeInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			count, err := m.scrapeAndWrite(ctx, cfg)
			if err != nil {
				m.setHealth("degraded", err.Error())
			} else {
				now := time.Now().UTC()
				m.mu.Lock()
				m.status = "online"
				m.message = "node metrics remote-write succeeded"
				m.lastScrapeAt = now
				m.lastWriteAt = now
				m.lastWriteCount = count
				m.mu.Unlock()
			}
			timer.Reset(interval)
		}
	}
}

func (m *Module) scrapeAndWrite(ctx context.Context, cfg config) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.NodeExporter.MetricsURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrape node_exporter: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, fmt.Errorf("scrape node_exporter: %s", resp.Status)
	}

	series, err := parsePrometheusText(resp.Body, time.Now().UTC(), cfg.Labels)
	if err != nil {
		return 0, err
	}
	if len(series) == 0 {
		return 0, fmt.Errorf("node_exporter scrape returned no remote-write samples")
	}
	if err := remoteWrite(ctx, m.httpClient, cfg.RemoteWriteURL, os.Getenv(m.apiKeyEnv), series); err != nil {
		return 0, err
	}
	return len(series), nil
}

func (m *Module) waitForNodeExporter(cmd *exec.Cmd) {
	err := cmd.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.process != cmd {
		return
	}
	m.process = nil
	if m.enabled {
		m.status = "degraded"
		if err != nil {
			m.message = "node_exporter exited: " + err.Error()
		} else {
			m.message = "node_exporter exited"
		}
	}
}

func (m *Module) stopActive(ctx context.Context) {
	m.mu.Lock()
	cancel := m.cancel
	cmd := m.process
	m.cancel = nil
	m.process = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
	case <-ctx.Done():
		_ = cmd.Process.Kill()
	}
}

func (m *Module) setHealth(status, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = status
	m.message = message
}

type config struct {
	RemoteWriteURL string
	ScrapeInterval time.Duration
	NodeExporter   nodeExporterConfig
	Labels         map[string]string
}

type nodeExporterConfig struct {
	ListenAddress string
	MetricsURL    string
	Artifact      tools.Artifact
}

func parseConfig(raw map[string]any) (config, error) {
	cfg := config{
		RemoteWriteURL: stringValue(raw["remote_write_url"]),
		ScrapeInterval: time.Duration(intValue(raw["scrape_interval_seconds"], 30)) * time.Second,
		Labels:         stringMap(raw["labels"]),
	}
	exporterRaw, _ := raw["node_exporter"].(map[string]any)
	cfg.NodeExporter = nodeExporterConfig{
		ListenAddress: stringValue(exporterRaw["listen_address"]),
		MetricsURL:    stringValue(exporterRaw["metrics_url"]),
	}
	if cfg.NodeExporter.ListenAddress == "" {
		cfg.NodeExporter.ListenAddress = "127.0.0.1:9100"
	}
	if cfg.NodeExporter.MetricsURL == "" {
		cfg.NodeExporter.MetricsURL = "http://127.0.0.1:9100/metrics"
	}
	artifactRaw, _ := exporterRaw["artifact"].(map[string]any)
	cfg.NodeExporter.Artifact = tools.Artifact{
		ToolName:     stringValue(artifactRaw["tool_name"]),
		Version:      stringValue(artifactRaw["version"]),
		Platform:     stringValue(artifactRaw["platform"]),
		Architecture: stringValue(artifactRaw["architecture"]),
		DownloadURL:  stringValue(artifactRaw["download_url"]),
		SHA256:       stringValue(artifactRaw["sha256"]),
		SizeBytes:    int64(intValue(artifactRaw["size_bytes"], 0)),
	}
	return cfg, nil
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(value)
	}
}

func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(typed)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func stringMap(value any) map[string]string {
	result := map[string]string{}
	raw, _ := value.(map[string]any)
	for key, item := range raw {
		itemValue := stringValue(item)
		if key != "" && itemValue != "" {
			result[key] = itemValue
		}
	}
	return result
}
