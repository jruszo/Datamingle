package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jruszo/datamingle/gateway/internal/auth"
)

func TestCortexHandlerRoutesToExactTargetPath(t *testing.T) {
	var gotPath string
	var gotQuery string
	var gotOrgID string

	cortex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotOrgID = r.Header.Get("X-Scope-OrgID")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer cortex.Close()

	handler := CortexHandler(cortex.URL + "/api/v1/push")

	request := httptest.NewRequest(http.MethodPost, "/api/v1/prometheus/write?ignored=1", nil)
	ctx := context.WithValue(request.Context(), auth.OrgIDKey, "org_test_123")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request.WithContext(ctx))

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, response.Code)
	}
	if gotPath != "/api/v1/push" {
		t.Fatalf("expected target path /api/v1/push, got %s", gotPath)
	}
	if gotQuery != "" {
		t.Fatalf("expected empty target query, got %s", gotQuery)
	}
	if gotOrgID != "org_test_123" {
		t.Fatalf("expected X-Scope-OrgID org_test_123, got %s", gotOrgID)
	}
}
