package monitoring

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func TestParsePrometheusTextAndRemoteWrite(t *testing.T) {
	series, err := parsePrometheusText(
		strings.NewReader("node_cpu_seconds_total{cpu=\"0\",mode=\"idle\"} 12.5\nnode_memory_MemAvailable_bytes 42\n"),
		time.Unix(100, 0).UTC(),
		map[string]string{"agent_id": "7", "node_name": "db-node-01"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 2 {
		t.Fatalf("expected two series, got %d", len(series))
	}

	received := make(chan prompb.WriteRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer key_123" {
			t.Errorf("unexpected authorization header %q", got)
		}
		if got := r.Header.Get("Content-Encoding"); got != "snappy" {
			t.Errorf("unexpected content encoding %q", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		decoded, err := snappy.Decode(nil, raw)
		if err != nil {
			t.Errorf("decode snappy: %v", err)
			return
		}
		var payload prompb.WriteRequest
		if err := proto.Unmarshal(decoded, &payload); err != nil {
			t.Errorf("unmarshal remote write: %v", err)
			return
		}
		received <- payload
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := remoteWrite(context.Background(), server.Client(), server.URL, "key_123", series); err != nil {
		t.Fatal(err)
	}
	payload := <-received
	if len(payload.Timeseries) != 2 {
		t.Fatalf("expected two remote-write series, got %d", len(payload.Timeseries))
	}
	if !hasLabel(payload.Timeseries[0].Labels, "agent_id", "7") {
		t.Fatalf("expected agent_id label in %+v", payload.Timeseries[0].Labels)
	}
}

func hasLabel(labels []prompb.Label, name string, value string) bool {
	for _, label := range labels {
		if label.Name == name && label.Value == value {
			return true
		}
	}
	return false
}
