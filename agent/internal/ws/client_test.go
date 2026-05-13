package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestClientSendsHelloAndHandlesMessages(t *testing.T) {
	helloCh := make(chan Hello, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/ws/agent/" {
			t.Errorf("unexpected websocket path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer key_123" {
			t.Errorf("unexpected authorization header %q", got)
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		var hello Hello
		if err := conn.ReadJSON(&hello); err != nil {
			t.Errorf("hello read failed: %v", err)
			return
		}
		helloCh <- hello

		if err := conn.WriteJSON(Message{Type: "hello.ack", Revision: 4}); err != nil {
			t.Errorf("hello ack write failed: %v", err)
			return
		}
		if err := conn.WriteJSON(Message{Type: "config.changed", Revision: 5, Reason: "assignment.updated"}); err != nil {
			t.Errorf("config changed write failed: %v", err)
			return
		}
	}))
	defer server.Close()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/ws/agent/"
	header := make(http.Header)
	header.Set("Authorization", "Bearer key_123")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var messages []Message
	client := Client{
		Endpoint: endpoint,
		Header:   header,
		Backoff:  NewBackoff(time.Millisecond, time.Millisecond),
		Hello: func() Hello {
			return Hello{AgentVersion: "test", ConfigRevision: 4}
		},
	}
	err := client.Run(ctx, func(_ context.Context, message Message) error {
		messages = append(messages, message)
		if message.Type == "config.changed" {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	hello := <-helloCh
	if hello.Type != "hello" || hello.AgentVersion != "test" || hello.ConfigRevision != 4 {
		t.Fatalf("unexpected hello: %+v", hello)
	}
	if len(messages) != 2 {
		t.Fatalf("expected two messages, got %d", len(messages))
	}
	if messages[1].Type != "config.changed" || messages[1].Revision != 5 {
		t.Fatalf("unexpected config message: %+v", messages[1])
	}
}
