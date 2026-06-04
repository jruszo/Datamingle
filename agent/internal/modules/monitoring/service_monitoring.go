package monitoring

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/jruszo/datamingle/agent/internal/modules"
	"github.com/jruszo/datamingle/agent/internal/tools"
)

var mysqlExporterCollectors = []string{
	"heartbeat.utc",
	"info_schema.processlist.processes_by_user",
	"info_schema.processlist.processes_by_host",
	"mysql.user.privileges",
	"perf_schema.indexiowaits",
	"perf_schema.tablelocks",
	"perf_schema.eventsstatements",
	"perf_schema.eventsstatementssum",
	"perf_schema.eventswaits",
	"heartbeat",
	"slave_hosts",
	"info_schema.replica_host",
	"info_schema.rocksdb_perf_context",
	"perf_schema.file_events",
	"perf_schema.file_instances",
	"perf_schema.memory_events",
	"perf_schema.replication_group_members",
	"perf_schema.replication_group_member_stats",
	"perf_schema.replication_applier_status_by_worker",
	"sys.user_summary",
	"info_schema.userstats",
	"info_schema.clientstats",
	"info_schema.tablestats",
	"info_schema.schemastats",
	"info_schema.innodb_cmp",
	"info_schema.innodb_cmpmem",
	"info_schema.query_response_time",
	"engine_tokudb_status",
	"engine_innodb_status",
	"global_status",
	"global_variables",
	"slave_status",
	"info_schema.processlist",
	"mysql.user",
	"info_schema.tables",
	"info_schema.innodb_tablespaces",
	"info_schema.innodb_metrics",
	"auto_increment.columns",
	"binlog_size",
	"perf_schema.tableiowaits",
}

var postgresExporterCollectors = []string{
	"buffercache_summary",
	"database",
	"database_wraparound",
	"locks",
	"long_running_transactions",
	"postmaster",
	"process_idle",
	"replication",
	"replication_slot",
	"roles",
	"stat_activity_autovacuum",
	"stat_bgwriter",
	"stat_checkpointer",
	"stat_database",
	"stat_progress_vacuum",
	"stat_statements",
	"stat_statements.include_query",
	"stat_user_tables",
	"stat_wal_receiver",
	"statio_user_indexes",
	"statio_user_tables",
	"wal",
	"xlog_location",
}

type ServiceModule struct {
	mu             sync.RWMutex
	dataDir        string
	apiKeyEnv      string
	httpClient     *http.Client
	cancel         context.CancelFunc
	processes      map[int64]*exec.Cmd
	enabled        bool
	revision       int64
	status         string
	message        string
	lastScrapeAt   time.Time
	lastWriteAt    time.Time
	lastWriteCount int
}

func NewService(dataDir, apiKeyEnv string) *ServiceModule {
	return &ServiceModule{
		dataDir:    dataDir,
		apiKeyEnv:  apiKeyEnv,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		processes:  map[int64]*exec.Cmd{},
		status:     "disabled",
		message:    "service monitoring is disabled",
	}
}

func (m *ServiceModule) Name() string {
	return "service_monitoring"
}

func (m *ServiceModule) Capabilities() []string {
	return []string{"service.metrics"}
}

func (m *ServiceModule) ApplyConfig(ctx context.Context, cfg modules.Config) error {
	m.stopActive(context.Background())

	m.mu.Lock()
	m.enabled = cfg.Enabled
	m.revision = cfg.Revision
	m.status = "disabled"
	m.message = "service monitoring is disabled"
	m.lastScrapeAt = time.Time{}
	m.lastWriteAt = time.Time{}
	m.lastWriteCount = 0
	m.mu.Unlock()

	if !cfg.Enabled {
		return nil
	}

	parsed, err := parseServiceMonitoringConfig(cfg.Raw)
	if err != nil {
		m.setHealth("degraded", err.Error())
		return nil
	}
	if parsed.RemoteWriteURL == "" {
		m.setHealth("degraded", "remote write URL is not configured")
		return nil
	}
	if len(parsed.Services) == 0 {
		m.setHealth("disabled", "no monitored services assigned")
		return nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	started := map[int64]*exec.Cmd{}
	for _, service := range parsed.Services {
		if service.Exporter.Artifact.DownloadURL == "" {
			cancel()
			m.stopProcesses(started)
			m.setHealth("degraded", fmt.Sprintf("%s exporter artifact is not configured", service.InstanceName))
			return nil
		}
		binaryPath, err := tools.EnsureArtifact(
			ctx,
			filepath.Join(m.dataDir, "tools"),
			service.Exporter.Artifact,
			m.httpClient,
		)
		if err != nil {
			cancel()
			m.stopProcesses(started)
			m.setHealth("degraded", fmt.Sprintf("%s exporter artifact unavailable: %v", service.InstanceName, err))
			return nil
		}

		cmd, err := serviceExporterCommand(binaryPath, service)
		if err != nil {
			cancel()
			m.stopProcesses(started)
			m.setHealth("degraded", err.Error())
			return nil
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			cancel()
			m.stopProcesses(started)
			m.setHealth("degraded", fmt.Sprintf("%s exporter failed to start: %v", service.InstanceName, err))
			return nil
		}
		started[service.AssignmentID] = cmd
	}

	m.mu.Lock()
	m.cancel = cancel
	m.processes = started
	m.status = "online"
	m.message = fmt.Sprintf("%d service exporters running", len(started))
	m.mu.Unlock()

	for assignmentID, cmd := range started {
		go m.waitForExporter(assignmentID, cmd)
	}
	go m.scrapeLoop(runCtx, parsed)
	return nil
}

func (m *ServiceModule) Health(ctx context.Context) modules.Health {
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
			"exporters":        len(m.processes),
			"last_scrape_at":   m.lastScrapeAt,
			"last_write_at":    m.lastWriteAt,
			"last_write_count": m.lastWriteCount,
		},
	}
}

func (m *ServiceModule) Stop(ctx context.Context) error {
	m.stopActive(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = false
	m.status = "disabled"
	m.message = "service monitoring is disabled"
	return nil
}

func (m *ServiceModule) scrapeLoop(ctx context.Context, cfg serviceMonitoringConfig) {
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
				m.message = "service metrics remote-write succeeded"
				m.lastScrapeAt = now
				m.lastWriteAt = now
				m.lastWriteCount = count
				m.mu.Unlock()
			}
			timer.Reset(interval)
		}
	}
}

func (m *ServiceModule) scrapeAndWrite(ctx context.Context, cfg serviceMonitoringConfig) (int, error) {
	total := 0
	for _, service := range cfg.Services {
		count, err := m.scrapeServiceAndWrite(ctx, cfg, service)
		if err != nil {
			return total, err
		}
		total += count
	}
	return total, nil
}

func (m *ServiceModule) scrapeServiceAndWrite(ctx context.Context, cfg serviceMonitoringConfig, service monitoredService) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, service.Exporter.MetricsURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrape %s exporter: %w", service.InstanceName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, fmt.Errorf("scrape %s exporter: %s", service.InstanceName, resp.Status)
	}

	labels := copyLabels(cfg.Labels)
	labels["assignment_id"] = strconv.FormatInt(service.AssignmentID, 10)
	labels["instance_id"] = strconv.FormatInt(service.InstanceID, 10)
	labels["instance_name"] = service.InstanceName
	labels["db_type"] = service.DBType
	labels["node_id"] = strconv.FormatInt(service.NodeID, 10)
	labels["node_name"] = service.NodeName
	labels["target_host"] = service.Host
	labels["target_port"] = strconv.Itoa(service.Port)

	series, err := parsePrometheusText(resp.Body, time.Now().UTC(), labels)
	if err != nil {
		return 0, err
	}
	if len(series) == 0 {
		return 0, fmt.Errorf("%s exporter scrape returned no remote-write samples", service.InstanceName)
	}
	if err := remoteWrite(ctx, m.httpClient, cfg.RemoteWriteURL, os.Getenv(m.apiKeyEnv), series); err != nil {
		return 0, err
	}
	return len(series), nil
}

func (m *ServiceModule) waitForExporter(assignmentID int64, cmd *exec.Cmd) {
	err := cmd.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processes[assignmentID] != cmd {
		return
	}
	delete(m.processes, assignmentID)
	if m.enabled {
		m.status = "degraded"
		if err != nil {
			m.message = "service exporter exited: " + err.Error()
		} else {
			m.message = "service exporter exited"
		}
	}
}

func (m *ServiceModule) stopActive(ctx context.Context) {
	m.mu.Lock()
	cancel := m.cancel
	processes := m.processes
	m.cancel = nil
	m.processes = map[int64]*exec.Cmd{}
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.stopProcesses(processes)
}

func (m *ServiceModule) stopProcesses(processes map[int64]*exec.Cmd) {
	for _, cmd := range processes {
		if cmd == nil || cmd.Process == nil {
			continue
		}
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func(cmd *exec.Cmd) {
			_ = cmd.Wait()
			close(done)
		}(cmd)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
	}
}

func (m *ServiceModule) setHealth(status, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = status
	m.message = message
}

type serviceMonitoringConfig struct {
	RemoteWriteURL string
	ScrapeInterval time.Duration
	Labels         map[string]string
	Services       []monitoredService
}

type monitoredService struct {
	AssignmentID int64
	InstanceID   int64
	InstanceName string
	NodeID       int64
	NodeName     string
	DBType       string
	Host         string
	Port         int
	Username     string
	Password     string
	Database     string
	Collectors   []string
	SSL          serviceSSLConfig
	Exporter     serviceExporterConfig
}

type serviceSSLConfig struct {
	Enabled bool
	Verify  bool
}

type serviceExporterConfig struct {
	ListenAddress string
	MetricsURL    string
	Artifact      tools.Artifact
}

func parseServiceMonitoringConfig(raw map[string]any) (serviceMonitoringConfig, error) {
	cfg := serviceMonitoringConfig{
		RemoteWriteURL: stringValue(raw["remote_write_url"]),
		ScrapeInterval: time.Duration(intValue(raw["scrape_interval_seconds"], 30)) * time.Second,
		Labels:         stringMap(raw["labels"]),
	}
	for _, item := range anyList(raw["services"]) {
		serviceRaw, _ := item.(map[string]any)
		exporterRaw, _ := serviceRaw["exporter"].(map[string]any)
		sslRaw, _ := serviceRaw["ssl"].(map[string]any)
		artifactRaw, _ := exporterRaw["artifact"].(map[string]any)
		service := monitoredService{
			AssignmentID: int64(intValue(serviceRaw["assignment_id"], 0)),
			InstanceID:   int64(intValue(serviceRaw["instance_id"], 0)),
			InstanceName: stringValue(serviceRaw["instance_name"]),
			NodeID:       int64(intValue(serviceRaw["node_id"], 0)),
			NodeName:     stringValue(serviceRaw["node_name"]),
			DBType:       stringValue(serviceRaw["db_type"]),
			Host:         stringValue(serviceRaw["host"]),
			Port:         intValue(serviceRaw["port"], 0),
			Username:     stringValue(serviceRaw["username"]),
			Password:     stringValue(serviceRaw["password"]),
			Database:     stringValue(serviceRaw["database"]),
			Collectors:   stringList(serviceRaw["collectors"]),
			SSL: serviceSSLConfig{
				Enabled: boolValue(sslRaw["enabled"], false),
				Verify:  boolValue(sslRaw["verify"], true),
			},
			Exporter: serviceExporterConfig{
				ListenAddress: stringValue(exporterRaw["listen_address"]),
				MetricsURL:    stringValue(exporterRaw["metrics_url"]),
				Artifact:      artifactFromRaw(artifactRaw),
			},
		}
		if service.Exporter.MetricsURL == "" && service.Exporter.ListenAddress != "" {
			service.Exporter.MetricsURL = "http://" + service.Exporter.ListenAddress + "/metrics"
		}
		cfg.Services = append(cfg.Services, service)
	}
	return cfg, nil
}

func serviceExporterCommand(binaryPath string, service monitoredService) (*exec.Cmd, error) {
	args := []string{"--web.listen-address=" + service.Exporter.ListenAddress}
	cmd := exec.Command(binaryPath, args...)
	cmd.Env = append(os.Environ(), serviceExporterEnv(service)...)
	switch service.DBType {
	case "mysql":
		args = append(args, "--mysqld.address="+service.Host+":"+strconv.Itoa(service.Port))
		if service.Username != "" {
			args = append(args, "--mysqld.username="+service.Username)
		}
		args = append(args, mysqlCollectorArgs(service.Collectors)...)
		cmd = exec.Command(binaryPath, args...)
		cmd.Env = append(os.Environ(), serviceExporterEnv(service)...)
		return cmd, nil
	case "pgsql":
		args = append(args, postgresCollectorArgs(service.Collectors)...)
		cmd = exec.Command(binaryPath, args...)
		cmd.Env = append(os.Environ(), serviceExporterEnv(service)...)
		return cmd, nil
	default:
		return nil, fmt.Errorf("unsupported service monitoring engine %q", service.DBType)
	}
}

func mysqlCollectorArgs(collectors []string) []string {
	selected := map[string]bool{}
	for _, collector := range collectors {
		selected[collector] = true
	}
	args := make([]string, 0, len(mysqlExporterCollectors))
	for _, collector := range mysqlExporterCollectors {
		if selected[collector] {
			args = append(args, "--collect."+collector)
		} else {
			args = append(args, "--no-collect."+collector)
		}
	}
	return args
}

func postgresCollectorArgs(collectors []string) []string {
	selected := map[string]bool{}
	for _, collector := range collectors {
		selected[collector] = true
	}
	args := make([]string, 0, len(postgresExporterCollectors))
	for _, collector := range postgresExporterCollectors {
		if selected[collector] {
			args = append(args, "--collector."+collector)
		} else {
			args = append(args, "--no-collector."+collector)
		}
	}
	return args
}

func serviceExporterEnv(service monitoredService) []string {
	switch service.DBType {
	case "mysql":
		return []string{"MYSQLD_EXPORTER_PASSWORD=" + service.Password}
	case "pgsql":
		return []string{
			"DATA_SOURCE_URI=" + postgresDataSourceURI(service),
			"DATA_SOURCE_USER=" + service.Username,
			"DATA_SOURCE_PASS=" + service.Password,
		}
	default:
		return nil
	}
}

func postgresDataSourceURI(service monitoredService) string {
	database := service.Database
	if database == "" {
		database = "postgres"
	}
	sslMode := "disable"
	if service.SSL.Enabled {
		sslMode = "require"
		if !service.SSL.Verify {
			sslMode = "require"
		}
	}
	query := url.Values{}
	query.Set("sslmode", sslMode)
	return fmt.Sprintf("%s:%d/%s?%s", service.Host, service.Port, database, query.Encode())
}

func artifactFromRaw(raw map[string]any) tools.Artifact {
	return tools.Artifact{
		ToolName:     stringValue(raw["tool_name"]),
		Version:      stringValue(raw["version"]),
		Platform:     stringValue(raw["platform"]),
		Architecture: stringValue(raw["architecture"]),
		DownloadURL:  stringValue(raw["download_url"]),
		SHA256:       stringValue(raw["sha256"]),
		SizeBytes:    int64(intValue(raw["size_bytes"], 0)),
	}
}

func anyList(value any) []any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	return items
}

func boolValue(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(typed)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func copyLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		result[key] = value
	}
	return result
}
