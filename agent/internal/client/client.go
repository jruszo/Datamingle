package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Client struct {
	baseURL    *url.URL
	apiKey     string
	httpClient HTTPDoer
}

func New(baseURL, apiKey string, httpClient HTTPDoer) (*Client, error) {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("base URL must include scheme and host")
	}
	return &Client{baseURL: parsed, apiKey: apiKey, httpClient: httpClient}, nil
}

func (c *Client) Register(ctx context.Context, payload RegisterRequest) (RegisterResponse, error) {
	var response RegisterResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/agent/register/", payload, &response); err != nil {
		return RegisterResponse{}, err
	}
	return response, nil
}

func (c *Client) FetchConfig(ctx context.Context) (AgentConfig, error) {
	var response AgentConfig
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/agent/me/config/", nil, &response); err != nil {
		return AgentConfig{}, err
	}
	response.Fetched = time.Now().UTC()
	return response, nil
}

func (c *Client) Heartbeat(ctx context.Context, payload HeartbeatRequest) (HeartbeatResponse, error) {
	var response HeartbeatResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/agent/me/heartbeat/", payload, &response); err != nil {
		return HeartbeatResponse{}, err
	}
	return response, nil
}

func (c *Client) FetchCommand(ctx context.Context, commandID int64) (AgentCommand, error) {
	var response AgentCommand
	path := fmt.Sprintf("/api/v1/agent/commands/%d/", commandID)
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &response); err != nil {
		return AgentCommand{}, err
	}
	return response, nil
}

func (c *Client) AckCommand(ctx context.Context, commandID int64) (CommandAckResponse, error) {
	var response CommandAckResponse
	path := fmt.Sprintf("/api/v1/agent/commands/%d/ack/", commandID)
	if err := c.doJSON(ctx, http.MethodPost, path, map[string]any{}, &response); err != nil {
		return CommandAckResponse{}, err
	}
	return response, nil
}

func (c *Client) StartCommand(ctx context.Context, commandID int64, payload CommandLeaseRequest) (CommandStatusResponse, error) {
	return c.commandStatusAction(ctx, commandID, "start", payload)
}

func (c *Client) ReportCommandProgress(ctx context.Context, commandID int64, payload CommandProgressRequest) (CommandStatusResponse, error) {
	return c.commandStatusAction(ctx, commandID, "progress", payload)
}

func (c *Client) FinishCommand(ctx context.Context, commandID int64, payload CommandFinishRequest) (CommandStatusResponse, error) {
	return c.commandStatusAction(ctx, commandID, "finish", payload)
}

func (c *Client) FailCommand(ctx context.Context, commandID int64, payload CommandFailRequest) (CommandStatusResponse, error) {
	return c.commandStatusAction(ctx, commandID, "fail", payload)
}

func (c *Client) MarkCommandCancelled(ctx context.Context, commandID int64, payload CommandFinishRequest) (CommandStatusResponse, error) {
	return c.commandStatusAction(ctx, commandID, "cancel", payload)
}

func (c *Client) Check(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/api/info"), nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("backend returned %s", resp.Status)
	}
	return nil
}

func (c *Client) WebsocketEndpoint() string {
	next := *c.baseURL
	switch next.Scheme {
	case "https":
		next.Scheme = "wss"
	case "http":
		next.Scheme = "ws"
	}
	next.Path = strings.TrimRight(next.Path, "/") + "/api/ws/agent/"
	next.RawQuery = ""
	return next.String()
}

func (c *Client) AuthorizationHeader() http.Header {
	header := make(http.Header)
	if c.apiKey != "" {
		header.Set("Authorization", "Bearer "+c.apiKey)
	}
	return header
}

func (c *Client) commandStatusAction(ctx context.Context, commandID int64, action string, payload any) (CommandStatusResponse, error) {
	var response CommandStatusResponse
	path := fmt.Sprintf("/api/v1/agent/commands/%d/%s/", commandID, action)
	if err := c.doJSON(ctx, http.MethodPost, path, payload, &response); err != nil {
		return CommandStatusResponse{}, err
	}
	return response, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, payload, out any) error {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s failed: %s: %s", method, path, resp.Status, strings.TrimSpace(string(raw)))
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return err
	}
	return nil
}

func (c *Client) endpoint(path string) string {
	next := *c.baseURL
	next.Path = strings.TrimRight(next.Path, "/") + path
	return next.String()
}
