package runtime

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	stdlibRuntime "runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jruszo/datamingle/agent/internal/client"
	"github.com/jruszo/datamingle/agent/internal/commands"
	"github.com/jruszo/datamingle/agent/internal/config"
	"github.com/jruszo/datamingle/agent/internal/modules"
	"github.com/jruszo/datamingle/agent/internal/modules/logs"
	"github.com/jruszo/datamingle/agent/internal/modules/monitoring"
	"github.com/jruszo/datamingle/agent/internal/modules/placeholder"
	"github.com/jruszo/datamingle/agent/internal/secrets"
	"github.com/jruszo/datamingle/agent/internal/tools"
	"github.com/jruszo/datamingle/agent/internal/version"
	agentws "github.com/jruszo/datamingle/agent/internal/ws"
)

type Runner struct {
	cfg             config.Config
	client          *client.Client
	apiKey          string
	modules         *modules.Manager
	executor        *commands.Executor
	configMu        sync.RWMutex
	config          client.AgentConfig
	commandMu       sync.Mutex
	runningCommands map[int64]context.CancelFunc
	revision        atomic.Int64
}

func NewRunner(cfg config.Config) *Runner {
	return &Runner{
		cfg:             cfg,
		executor:        commands.NewExecutorWithToolCache(filepath.Join(cfg.DataDir, "tools")),
		runningCommands: make(map[int64]context.CancelFunc),
		modules: modules.NewManager(
			placeholder.New("mysql", []string{"connection.test", "inventory.collect", "query.execute"}),
			placeholder.New("metrics", []string{"metrics.export"}),
			placeholder.New("online_schema", []string{"schema.change"}),
			monitoring.New(cfg.DataDir, cfg.APIKeyEnv),
			monitoring.NewService(cfg.DataDir, cfg.APIKeyEnv),
			logs.New(),
		),
	}
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.RunOnce(ctx); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.modules.Stop(cleanupCtx); err != nil {
			log.Printf("agent module shutdown failed: %v", err)
		}
	}()

	heartbeat := time.NewTicker(30 * time.Second)
	refresh := time.NewTicker(5 * time.Minute)
	defer heartbeat.Stop()
	defer refresh.Stop()

	websocketCtx, stopWebsocket := context.WithCancel(ctx)
	defer stopWebsocket()
	websocketMessages := make(chan agentws.Message, 16)
	go r.listenWebsocket(websocketCtx, websocketMessages)

	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-websocketMessages:
			if err := r.handleWebsocketMessage(ctx, message); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("agent websocket message handling failed: %v", err)
			}
		case <-heartbeat.C:
			if err := r.sendHeartbeat(ctx, "online", 0); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("agent heartbeat failed: %v", err)
			}
		case <-refresh.C:
			if err := r.refreshConfig(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("agent config refresh failed: %v", err)
			}
		}
	}
}

func (r *Runner) RunOnce(ctx context.Context) error {
	installID, err := secrets.LoadOrCreateInstallID(r.cfg.DataDir)
	if err != nil {
		return err
	}
	apiClient, err := r.apiClient()
	if err != nil {
		return err
	}
	r.client = apiClient

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = r.cfg.AgentName
	}
	registration, err := r.client.Register(ctx, client.RegisterRequest{
		InstallID:    installID,
		Name:         r.cfg.AgentName,
		Address:      detectPrimaryAddress(),
		Hostname:     hostname,
		Platform:     stdlibRuntime.GOOS,
		Architecture: stdlibRuntime.GOARCH,
		AgentVersion: version.Version,
	})
	if err != nil {
		return err
	}

	configPayload, err := r.client.FetchConfig(ctx)
	if err != nil {
		return err
	}
	if configPayload.Revision == 0 {
		configPayload.Revision = registration.DesiredConfigRevision
	}
	if err := r.applyConfig(ctx, configPayload); err != nil {
		return err
	}
	r.setConfig(configPayload)
	r.revision.Store(configPayload.Revision)
	_, err = r.client.Heartbeat(ctx, client.HeartbeatRequest{
		InstallID:      installID,
		Status:         "online",
		ConfigRevision: configPayload.Revision,
		ModuleHealth:   toClientHealth(r.modules.Health(ctx)),
	})
	return err
}

func detectPrimaryAddress() string {
	var fallback string
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagUp == 0 || networkInterface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := networkInterface.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			ipNet, ok := address.(*net.IPNet)
			if !ok || ipNet.IP.IsLoopback() {
				continue
			}
			if ipv4 := ipNet.IP.To4(); ipv4 != nil {
				return ipv4.String()
			}
			if fallback == "" {
				fallback = ipNet.IP.String()
			}
		}
	}
	return fallback
}

func (r *Runner) CheckConnectivity(ctx context.Context) error {
	apiClient, err := r.apiClient()
	if err != nil {
		return err
	}
	return apiClient.Check(ctx)
}

func (r *Runner) refreshConfig(ctx context.Context) error {
	if r.client == nil {
		return fmt.Errorf("agent client is not initialized")
	}
	payload, err := r.client.FetchConfig(ctx)
	if err != nil {
		return err
	}
	if err := r.applyConfig(ctx, payload); err != nil {
		return err
	}
	r.setConfig(payload)
	r.revision.Store(payload.Revision)
	return nil
}

func (r *Runner) sendHeartbeat(ctx context.Context, status string, revision int64) error {
	if r.client == nil {
		return fmt.Errorf("agent client is not initialized")
	}
	installID, err := secrets.LoadInstallID(r.cfg.DataDir)
	if err != nil {
		return err
	}
	_, err = r.client.Heartbeat(ctx, client.HeartbeatRequest{
		InstallID:      installID,
		Status:         status,
		ConfigRevision: r.effectiveRevision(revision),
		ModuleHealth:   toClientHealth(r.modules.Health(ctx)),
	})
	return err
}

func (r *Runner) effectiveRevision(revision int64) int64 {
	if revision > 0 {
		return revision
	}
	return r.revision.Load()
}

func (r *Runner) apiClient() (*client.Client, error) {
	apiKey := os.Getenv(r.cfg.APIKeyEnv)
	if apiKey == "" {
		return nil, fmt.Errorf("missing API key environment variable %s", r.cfg.APIKeyEnv)
	}
	r.apiKey = apiKey
	return client.New(r.cfg.DatamingleURL, apiKey, nil)
}

func (r *Runner) applyConfig(ctx context.Context, payload client.AgentConfig) error {
	logMonitoringExpectation(payload)
	if err := r.reconcileToolArtifacts(ctx, payload); err != nil {
		return err
	}
	configs := make([]modules.Config, 0, len(payload.Modules))
	for _, module := range payload.Modules {
		configs = append(configs, modules.Config{
			Name:     module.Name,
			Enabled:  module.Enabled,
			Revision: module.Revision,
			Raw:      module.Raw,
		})
	}
	return r.modules.Apply(ctx, configs)
}

func logMonitoringExpectation(payload client.AgentConfig) {
	for _, module := range payload.Modules {
		if module.Name != "service_monitoring" {
			continue
		}
		services, _ := module.Raw["services"].([]any)
		log.Printf("service monitoring exporters expected: %d", len(services))
		break
	}
	if payload.Node != nil {
		log.Printf(
			"node monitoring expected for %s (%d): %t",
			payload.Node.Name,
			payload.Node.ID,
			payload.Node.MonitoringEnabled,
		)
		return
	}
	if len(payload.Nodes) == 0 {
		log.Printf("node monitoring expected: false (no node assigned)")
		return
	}
	for _, node := range payload.Nodes {
		log.Printf(
			"node monitoring expected for %s (%d): %t",
			node.Name,
			node.ID,
			node.MonitoringEnabled,
		)
	}
}

func (r *Runner) reconcileToolArtifacts(ctx context.Context, payload client.AgentConfig) error {
	if !moduleEnabled(payload.Modules, "online_schema") {
		return nil
	}
	cacheDir := filepath.Join(r.cfg.DataDir, "tools")
	requiredTools := map[string]bool{
		"gh-ost":                  false,
		"pt-online-schema-change": false,
	}
	for _, artifact := range payload.ToolArtifacts {
		if artifact.Platform != stdlibRuntime.GOOS || artifact.Architecture != stdlibRuntime.GOARCH {
			continue
		}
		if _, required := requiredTools[artifact.ToolName]; !required {
			continue
		}
		_, err := tools.EnsureArtifact(ctx, cacheDir, tools.Artifact{
			ToolName:     artifact.ToolName,
			Version:      artifact.Version,
			Platform:     artifact.Platform,
			Architecture: artifact.Architecture,
			DownloadURL:  artifact.DownloadURL,
			SHA256:       artifact.SHA256,
			SizeBytes:    artifact.SizeBytes,
		}, nil)
		if err != nil {
			return fmt.Errorf("sync tool artifact %s %s: %w", artifact.ToolName, artifact.Version, err)
		}
		requiredTools[artifact.ToolName] = true
	}
	for toolName, found := range requiredTools {
		if !found {
			return fmt.Errorf("%s artifact is not configured for %s/%s", toolName, stdlibRuntime.GOOS, stdlibRuntime.GOARCH)
		}
	}
	return nil
}

func moduleEnabled(configs []client.ModuleConfig, name string) bool {
	for _, cfg := range configs {
		if cfg.Name == name && cfg.Enabled {
			return true
		}
	}
	return false
}

func (r *Runner) listenWebsocket(ctx context.Context, messages chan<- agentws.Message) {
	if r.client == nil || r.apiKey == "" {
		return
	}
	listener := agentws.Client{
		Endpoint: r.client.WebsocketEndpoint(),
		Header:   r.client.AuthorizationHeader(),
		Backoff:  agentws.NewBackoff(time.Second, time.Minute),
		Hello: func() agentws.Hello {
			return agentws.Hello{
				AgentVersion:   version.Version,
				ConfigRevision: r.revision.Load(),
			}
		},
	}
	_ = listener.Run(ctx, func(ctx context.Context, message agentws.Message) error {
		select {
		case messages <- message:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func (r *Runner) handleWebsocketMessage(ctx context.Context, message agentws.Message) error {
	switch message.Type {
	case "hello.ack":
		return nil
	case "config.changed":
		return r.refreshConfig(ctx)
	case "command.available":
		go func() {
			_ = r.handleCommandAvailable(ctx, message)
		}()
		return nil
	case "command.cancel":
		return r.handleCommandCancel(message)
	default:
		return nil
	}
}

func (r *Runner) handleCommandAvailable(ctx context.Context, message agentws.Message) error {
	if r.client == nil {
		return fmt.Errorf("agent client is not initialized")
	}
	if message.CommandID == 0 {
		return nil
	}
	command, err := r.client.FetchCommand(ctx, message.CommandID)
	if err != nil {
		return err
	}
	if _, err := r.client.AckCommand(ctx, command.ID); err != nil {
		return err
	}
	return r.executeCommand(ctx, command)
}

func (r *Runner) handleCommandCancel(message agentws.Message) error {
	if message.CommandID == 0 {
		return nil
	}
	r.commandMu.Lock()
	cancel := r.runningCommands[message.CommandID]
	r.commandMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (r *Runner) executeCommand(ctx context.Context, command client.AgentCommand) error {
	leaseOwner := r.cfg.AgentName
	if leaseOwner == "" {
		leaseOwner = "datamingle-agent"
	}
	executionCtx, cancel := context.WithCancel(ctx)
	if !r.registerRunningCommand(command.ID, cancel) {
		cancel()
		return nil
	}
	defer r.unregisterRunningCommand(command.ID, cancel)

	if command.CancelRequested {
		return r.markCommandCancelled(ctx, command.ID, "command cancellation requested")
	}

	if _, err := r.client.StartCommand(ctx, command.ID, client.CommandLeaseRequest{
		LeaseOwner:   leaseOwner,
		LeaseSeconds: 300,
	}); err != nil {
		return err
	}
	_, _ = r.client.ReportCommandProgress(ctx, command.ID, client.CommandProgressRequest{
		LeaseOwner: leaseOwner,
		Message:    "command execution started",
		Payload: map[string]any{
			"command_type": command.CommandType,
		},
	})

	result, err := r.executor.Execute(executionCtx, command, r.currentConfig())
	if err != nil {
		if executionCtx.Err() != nil {
			return r.markCommandCancelled(ctx, command.ID, "command cancelled")
		}
		_, reportErr := r.client.FailCommand(ctx, command.ID, client.CommandFailRequest{
			Message: err.Error(),
			Error: map[string]any{
				"message": err.Error(),
			},
		})
		return reportErr
	}
	_, err = r.client.FinishCommand(ctx, command.ID, client.CommandFinishRequest{
		Message: result.Message,
		Result:  result.Payload,
	})
	return err
}

func (r *Runner) markCommandCancelled(ctx context.Context, commandID int64, message string) error {
	_, err := r.client.MarkCommandCancelled(ctx, commandID, client.CommandFinishRequest{
		Message: message,
		Result: map[string]any{
			"cancelled": true,
		},
	})
	return err
}

func (r *Runner) registerRunningCommand(commandID int64, cancel context.CancelFunc) bool {
	r.commandMu.Lock()
	defer r.commandMu.Unlock()
	if _, exists := r.runningCommands[commandID]; exists {
		return false
	}
	r.runningCommands[commandID] = cancel
	return true
}

func (r *Runner) unregisterRunningCommand(commandID int64, cancel context.CancelFunc) {
	r.commandMu.Lock()
	delete(r.runningCommands, commandID)
	r.commandMu.Unlock()
	cancel()
}

func (r *Runner) setConfig(payload client.AgentConfig) {
	r.configMu.Lock()
	defer r.configMu.Unlock()
	r.config = payload
}

func (r *Runner) currentConfig() client.AgentConfig {
	r.configMu.RLock()
	defer r.configMu.RUnlock()
	return r.config
}

func toClientHealth(values []modules.Health) []client.ModuleHealth {
	health := make([]client.ModuleHealth, 0, len(values))
	for _, value := range values {
		health = append(health, client.ModuleHealth{
			Module:    value.Module,
			Status:    value.Status,
			Message:   value.Message,
			UpdatedAt: value.UpdatedAt,
			Details:   value.Details,
		})
	}
	return health
}
