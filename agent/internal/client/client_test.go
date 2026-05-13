package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestRegisterSendsBearerTokenAndPayload(t *testing.T) {
	doer := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v1/agent/register/" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer key_123" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		var payload RegisterRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload.InstallID != "ins_123" {
			t.Fatalf("unexpected install id %q", payload.InstallID)
		}
		return jsonResponse(t, RegisterResponse{AgentID: 7, DesiredConfigRevision: 3}), nil
	})

	c, err := New("https://datamingle.example.com", "key_123", doer)
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Register(context.Background(), RegisterRequest{InstallID: "ins_123"})
	if err != nil {
		t.Fatal(err)
	}
	if response.AgentID != 7 || response.DesiredConfigRevision != 3 {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestFetchConfigSetsFetchedTime(t *testing.T) {
	doer := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(t, AgentConfig{AgentID: 7, Revision: 4}), nil
	})

	c, err := New("https://datamingle.example.com", "", doer)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := c.FetchConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Fetched.IsZero() {
		t.Fatal("expected fetched timestamp")
	}
}

func TestFetchAndAckCommandUseAgentCommandEndpoints(t *testing.T) {
	var paths []string
	doer := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/agent/commands/42/":
			return jsonResponse(t, AgentCommand{ID: 42, CommandType: "connection.test"}), nil
		case "/api/v1/agent/commands/42/ack/":
			return jsonResponse(t, CommandAckResponse{Status: "accepted"}), nil
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
			return nil, nil
		}
	})

	c, err := New("https://datamingle.example.com", "key_123", doer)
	if err != nil {
		t.Fatal(err)
	}
	command, err := c.FetchCommand(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if command.ID != 42 || command.CommandType != "connection.test" {
		t.Fatalf("unexpected command response: %+v", command)
	}
	ack, err := c.AckCommand(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != "accepted" {
		t.Fatalf("unexpected ack response: %+v", ack)
	}
	if strings.Join(paths, ",") != "/api/v1/agent/commands/42/,/api/v1/agent/commands/42/ack/" {
		t.Fatalf("unexpected request path order: %v", paths)
	}
}

func TestCommandLifecycleMethodsUseAgentCommandEndpoints(t *testing.T) {
	var paths []string
	doer := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		return jsonResponse(t, CommandStatusResponse{Status: "ok"}), nil
	})

	c, err := New("https://datamingle.example.com", "key_123", doer)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	actions := []struct {
		name string
		call func() error
	}{
		{
			name: "start",
			call: func() error {
				_, err := c.StartCommand(ctx, 42, CommandLeaseRequest{LeaseOwner: "worker-1"})
				return err
			},
		},
		{
			name: "progress",
			call: func() error {
				_, err := c.ReportCommandProgress(ctx, 42, CommandProgressRequest{Message: "running"})
				return err
			},
		},
		{
			name: "finish",
			call: func() error {
				_, err := c.FinishCommand(ctx, 42, CommandFinishRequest{Result: map[string]any{"ok": true}})
				return err
			},
		},
		{
			name: "fail",
			call: func() error {
				_, err := c.FailCommand(ctx, 42, CommandFailRequest{Error: map[string]any{"message": "failed"}})
				return err
			},
		},
		{
			name: "cancelled",
			call: func() error {
				_, err := c.MarkCommandCancelled(ctx, 42, CommandFinishRequest{Message: "cancelled"})
				return err
			},
		},
	}

	for _, action := range actions {
		if err := action.call(); err != nil {
			t.Fatalf("%s failed: %v", action.name, err)
		}
	}
	expected := strings.Join([]string{
		"/api/v1/agent/commands/42/start/",
		"/api/v1/agent/commands/42/progress/",
		"/api/v1/agent/commands/42/finish/",
		"/api/v1/agent/commands/42/fail/",
		"/api/v1/agent/commands/42/cancelled/",
	}, ",")
	if strings.Join(paths, ",") != expected {
		t.Fatalf("unexpected request path order: %v", paths)
	}
}

func TestWebsocketEndpointAndAuthorizationHeader(t *testing.T) {
	c, err := New("https://datamingle.example.com/base", "key_123", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.WebsocketEndpoint(); got != "wss://datamingle.example.com/base/api/ws/agent/" {
		t.Fatalf("unexpected websocket endpoint %q", got)
	}
	if got := c.AuthorizationHeader().Get("Authorization"); got != "Bearer key_123" {
		t.Fatalf("unexpected authorization header %q", got)
	}
}

func TestAssignmentRedactsPasswordWhenFormattedAndMarshaled(t *testing.T) {
	assignment := Assignment{
		Username: "root",
		Password: "supersecret",
		Host:     "db.example.com",
	}

	raw, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{string(raw), assignment.String()} {
		if strings.Contains(output, "supersecret") {
			t.Fatalf("password leaked in %q", output)
		}
		if !strings.Contains(output, "redacted") {
			t.Fatalf("expected redacted marker in %q", output)
		}
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
