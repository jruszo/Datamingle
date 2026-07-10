package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	stdlibRuntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jruszo/datamingle/agent/internal/client"
	"github.com/jruszo/datamingle/agent/internal/config"
	agentws "github.com/jruszo/datamingle/agent/internal/ws"
)

func TestHandleConfigChangedRefreshesConfig(t *testing.T) {
	apiClient, err := client.New("https://datamingle.example.com", "key_123", roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v1/agent/me/config/" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		return jsonResponse(t, client.AgentConfig{AgentID: 7, Revision: 9}), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(config.Config{})
	runner.client = apiClient

	err = runner.handleWebsocketMessage(context.Background(), agentws.Message{Type: "config.changed"})
	if err != nil {
		t.Fatal(err)
	}
	if got := runner.effectiveRevision(0); got != 9 {
		t.Fatalf("expected revision 9, got %d", got)
	}
}

func TestApplyConfigLogsMonitoringExpectation(t *testing.T) {
	var buffer bytes.Buffer
	previousWriter := log.Writer()
	log.SetOutput(&buffer)
	defer log.SetOutput(previousWriter)

	runner := NewRunner(config.Config{})
	err := runner.applyConfig(context.Background(), client.AgentConfig{
		Node: &client.NodeConfig{
			ID:                7,
			Name:              "db-node-01",
			MonitoringEnabled: false,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buffer.String(), "node monitoring expected for db-node-01 (7): false") {
		t.Fatalf("expected monitoring log, got %q", buffer.String())
	}
}

func TestApplyConfigAllowsOnlineSchemaModuleWithoutEveryToolArtifact(t *testing.T) {
	runner := NewRunner(config.Config{DataDir: t.TempDir()})

	err := runner.applyConfig(context.Background(), client.AgentConfig{
		Modules: []client.ModuleConfig{
			{Name: "online_schema", Enabled: true, Revision: 3},
		},
	})

	if err != nil {
		t.Fatalf("expected config without declared artifacts to apply, got %v", err)
	}
}

func TestReconcileToolArtifactsDownloadsArchiverWithoutOnlineSchema(t *testing.T) {
	body := []byte("#!/usr/bin/env perl\n")
	sum := sha256.Sum256(body)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write(body)
	}))
	defer server.Close()

	dataDir := t.TempDir()
	runner := NewRunner(config.Config{DataDir: dataDir})
	artifacts := []client.ToolArtifact{
		{
			ToolName: "pt-archiver", Version: "3.7.1-4",
			Platform: stdlibRuntime.GOOS, Architecture: stdlibRuntime.GOARCH,
			DownloadURL: server.URL + "/pt-archiver", SHA256: hex.EncodeToString(sum[:]),
		},
		{
			ToolName: "pt-online-schema-change", Version: "3.7.1-4",
			Platform: stdlibRuntime.GOOS, Architecture: stdlibRuntime.GOARCH,
			DownloadURL: server.URL + "/pt-online-schema-change", SHA256: hex.EncodeToString(sum[:]),
		},
	}

	if err := runner.reconcileToolArtifacts(context.Background(), client.AgentConfig{ToolArtifacts: artifacts}); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("expected only pt-archiver to be downloaded, got %d downloads", requests)
	}
	path := filepath.Join(dataDir, "tools", "pt-archiver", "3.7.1-4", stdlibRuntime.GOOS+"-"+stdlibRuntime.GOARCH, "pt-archiver")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected cached pt-archiver at %s: %v", path, err)
	}
}

func TestHandleCommandAvailableFetchesAcksStartsAndReportsFailure(t *testing.T) {
	var paths []string
	var pathsMu sync.Mutex
	done := make(chan struct{})
	apiClient, err := client.New("https://datamingle.example.com", "key_123", roundTripFunc(func(r *http.Request) (*http.Response, error) {
		pathsMu.Lock()
		paths = append(paths, r.URL.Path)
		pathsMu.Unlock()
		switch r.URL.Path {
		case "/api/v1/agent/commands/42/":
			return jsonResponse(t, client.AgentCommand{ID: 42, InstanceID: 7, CommandType: "unsupported"}), nil
		case "/api/v1/agent/commands/42/ack/":
			return jsonResponse(t, client.CommandAckResponse{Status: "accepted"}), nil
		case "/api/v1/agent/commands/42/start/":
			return jsonResponse(t, client.CommandStatusResponse{Status: "running"}), nil
		case "/api/v1/agent/commands/42/progress/":
			return jsonResponse(t, client.CommandStatusResponse{Status: "running"}), nil
		case "/api/v1/agent/commands/42/fail/":
			close(done)
			return jsonResponse(t, client.CommandStatusResponse{Status: "failed"}), nil
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
			return nil, nil
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(config.Config{})
	runner.client = apiClient
	runner.setConfig(client.AgentConfig{
		Assignments: []client.Assignment{{InstanceID: 7, DBType: "mysql"}},
	})

	err = runner.handleWebsocketMessage(context.Background(), agentws.Message{
		Type:      "command.available",
		CommandID: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command failure report")
	}
	expected := strings.Join([]string{
		"/api/v1/agent/commands/42/",
		"/api/v1/agent/commands/42/ack/",
		"/api/v1/agent/commands/42/start/",
		"/api/v1/agent/commands/42/progress/",
		"/api/v1/agent/commands/42/fail/",
	}, ",")
	pathsMu.Lock()
	defer pathsMu.Unlock()
	if strings.Join(paths, ",") != expected {
		t.Fatalf("unexpected request path order: %v", paths)
	}
}

func TestHandleCommandCancelCancelsRunningCommand(t *testing.T) {
	runner := NewRunner(config.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	if !runner.registerRunningCommand(42, cancel) {
		t.Fatal("expected command registration to succeed")
	}
	defer runner.unregisterRunningCommand(42, cancel)

	err := runner.handleWebsocketMessage(context.Background(), agentws.Message{
		Type:      "command.cancel",
		CommandID: 42,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command cancellation")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(t *testing.T, value any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON response: %v", err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(string(raw))),
		Header:     make(http.Header),
	}
}
