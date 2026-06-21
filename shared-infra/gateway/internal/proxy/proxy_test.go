package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jruszo/datamingle/gateway/internal/auth"
)

func TestMetricsHandlerRoutesToTenantTargetPath(t *testing.T) {
	var gotPath string
	var gotQuery string
	var gotScopeHeader string

	metricsBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotScopeHeader = r.Header.Get("X-Scope-OrgID")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer metricsBackend.Close()

	backends := NewTenantBackendResolver(
		"http://default.example.test",
		map[string]string{"org_test_123": metricsBackend.URL},
	)
	handler := MetricsHandler(backends, "/api/v1/write")

	request := httptest.NewRequest(http.MethodPost, "/api/v1/prometheus/write?ignored=1", nil)
	ctx := context.WithValue(request.Context(), auth.OrgIDKey, "org_test_123")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request.WithContext(ctx))

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, response.Code)
	}
	if gotPath != "/api/v1/write" {
		t.Fatalf("expected target path /api/v1/write, got %s", gotPath)
	}
	if gotQuery != "" {
		t.Fatalf("expected empty target query, got %s", gotQuery)
	}
	if gotScopeHeader != "" {
		t.Fatalf("expected no X-Scope-OrgID header, got %s", gotScopeHeader)
	}
}
