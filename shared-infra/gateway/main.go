package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jruszo/datamingle/gateway/internal/auth"
	"github.com/jruszo/datamingle/gateway/internal/cache"
	"github.com/jruszo/datamingle/gateway/internal/proxy"
)

func main() {
	cortexEndpoint := os.Getenv("CORTEX_ENDPOINT")
	if cortexEndpoint == "" {
		cortexEndpoint = "http://cortex:9009"
	}

	workosAPIKey := os.Getenv("WORKOS_API_KEY")
	if workosAPIKey == "" {
		slog.Error("WORKOS_API_KEY environment variable is required")
		os.Exit(1)
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://redis:6379"
	}

	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "4430"
	}

	apiKeyCache, err := cache.NewAPIKeyCache(redisURL, 5*time.Minute)
	if err != nil {
		slog.Error("failed to create api key cache", "error", err)
		os.Exit(1)
	}

	authenticator := auth.NewAuthenticator(workosAPIKey, apiKeyCache)

	promWriteProxy := proxy.CortexHandler(cortexEndpoint + "/api/v1/push")
	otlpMetricsProxy := proxy.CortexHandler(cortexEndpoint + "/otlp/v1/metrics")

	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/prometheus/write", authenticator.Middleware(promWriteProxy))
	mux.Handle("POST /otlp/v1/metrics", authenticator.Middleware(otlpMetricsProxy))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	slog.Info("gateway starting",
		"port", port,
		"cortex_endpoint", cortexEndpoint,
	)

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
