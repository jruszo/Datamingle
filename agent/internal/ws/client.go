package ws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type Message struct {
	Type        string `json:"type"`
	Revision    int64  `json:"revision,omitempty"`
	Reason      string `json:"reason,omitempty"`
	CommandID   int64  `json:"command_id,omitempty"`
	CommandType string `json:"command_type,omitempty"`
	SentAt      string `json:"sent_at,omitempty"`
}

type Hello struct {
	Type           string `json:"type"`
	AgentVersion   string `json:"agent_version"`
	ConfigRevision int64  `json:"config_revision"`
}

type Handler func(context.Context, Message) error

type Dialer interface {
	DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (*websocket.Conn, *http.Response, error)
}

type Client struct {
	Endpoint string
	Header   http.Header
	Dialer   Dialer
	Backoff  Backoff
	Hello    func() Hello
}

func (c Client) Run(ctx context.Context, handler Handler) error {
	if handler == nil {
		return fmt.Errorf("websocket handler is required")
	}
	if c.Endpoint == "" {
		return fmt.Errorf("websocket endpoint is required")
	}
	if c.Dialer == nil {
		c.Dialer = websocket.DefaultDialer
	}
	if c.Backoff.Base == 0 && c.Backoff.Max == 0 {
		c.Backoff = NewBackoff(time.Second, time.Minute)
	}

	for attempt := 0; ; attempt++ {
		err := c.runOnce(ctx, handler)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			attempt = 0
			continue
		}
		delay := c.Backoff.Delay(attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (c Client) runOnce(ctx context.Context, handler Handler) error {
	conn, response, err := c.Dialer.DialContext(ctx, c.Endpoint, c.Header)
	if err != nil {
		if response != nil {
			if response.Body != nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
			return fmt.Errorf("websocket dial failed: %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()

	if c.Hello != nil {
		hello := c.Hello()
		if hello.Type == "" {
			hello.Type = "hello"
		}
		if err := conn.WriteJSON(hello); err != nil {
			return err
		}
	}

	for {
		var message Message
		if err := conn.ReadJSON(&message); err != nil {
			return err
		}
		if message.Type == "" {
			continue
		}
		if message.Type == "ping" {
			if err := conn.WriteJSON(Message{Type: "pong", SentAt: message.SentAt}); err != nil {
				return err
			}
			continue
		}
		if err := handler(ctx, message); err != nil {
			return err
		}
	}
}
