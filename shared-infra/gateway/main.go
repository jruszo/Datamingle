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

const (
	defaultCortexEndpoint = "http://cortex:9009"
	defaultRedisURL       = "redis://redis:6379"
	defaultGatewayPort    = "4430"

	prometheusRemoteWritePath = "/api/v1/push"
	otlpMetricsPath           = "/api/v1/otlp/v1/metrics"
)

func main() {
	cortexEndpoint := os.Getenv("CORTEX_ENDPOINT")
	if cortexEndpoint == "" {
		cortexEndpoint = defaultCortexEndpoint
	}

	workosAPIKey := os.Getenv("WORKOS_API_KEY")
	if workosAPIKey == "" {
		slog.Error("WORKOS_API_KEY environment variable is required")
		os.Exit(1)
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = defaultRedisURL
	}

	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = defaultGatewayPort
	}

	apiKeyCache, err := cache.NewAPIKeyCache(redisURL, 5*time.Minute)
	if err != nil {
		slog.Error("failed to create api key cache", "error", err)
		os.Exit(1)
	}

	authenticator := auth.NewAuthenticator(workosAPIKey, apiKeyCache)

	promWriteProxy := proxy.CortexHandler(cortexEndpoint + prometheusRemoteWritePath)
	otlpMetricsProxy := proxy.CortexHandler(cortexEndpoint + otlpMetricsPath)

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

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
