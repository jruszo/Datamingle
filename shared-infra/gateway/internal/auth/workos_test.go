package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/workos/workos-go/v9"
)

func TestMiddlewareRequiresIngestPermission(t *testing.T) {
	tests := []struct {
		name            string
		permissions     []string
		expectedStatus  int
		expectedHandler bool
	}{
		{
			name: "allows api key with ingest permission",
			permissions: []string{
				"datamingle-agent:connect",
				RequiredIngestPermission,
			},
			expectedStatus:  http.StatusNoContent,
			expectedHandler: true,
		},
		{
			name: "rejects api key without ingest permission",
			permissions: []string{
				"datamingle-agent:connect",
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedHandler: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workosServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Fatalf("expected POST, got %s", r.Method)
				}
				if r.URL.Path != "/api_keys/validations" {
					t.Fatalf("expected /api_keys/validations, got %s", r.URL.Path)
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(apiKeyValidationResponse(tt.permissions)))
			}))
			defer workosServer.Close()

			authenticator := newAuthenticatorWithClient(
				workos.NewClient("sk_test", workos.WithBaseURL(workosServer.URL)),
				nil,
			)

			handlerCalled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerCalled = true
				orgID, ok := OrgIDFromContext(r.Context())
				if !ok {
					t.Fatal("expected org_id in request context")
				}
				if orgID != "org_test_123" {
					t.Fatalf("expected org_test_123, got %s", orgID)
				}
				w.WriteHeader(http.StatusNoContent)
			})

			request := httptest.NewRequest(http.MethodPost, "/otlp/v1/metrics", nil)
			request.Header.Set("Authorization", "Bearer sk_agent_value")
			response := httptest.NewRecorder()

			authenticator.Middleware(next).ServeHTTP(response, request)

			if response.Code != tt.expectedStatus {
				t.Fatalf("expected status %d, got %d", tt.expectedStatus, response.Code)
			}
			if handlerCalled != tt.expectedHandler {
				t.Fatalf("expected handler called=%t, got %t", tt.expectedHandler, handlerCalled)
			}
		})
	}
}

func apiKeyValidationResponse(permissions []string) string {
	payload := map[string]any{
		"api_key": map[string]any{
			"object": "api_key",
			"id":     "api_key_123",
			"owner": map[string]string{
				"type": "organization",
				"id":   "org_test_123",
			},
			"name":             "Datamingle Agent",
			"obfuscated_value": "sk_...test",
			"last_used_at":     nil,
			"permissions":      permissions,
			"created_at":       "2026-01-15T12:00:00.000Z",
			"updated_at":       "2026-01-15T12:00:00.000Z",
			"expires_at":       nil,
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(data)
}
