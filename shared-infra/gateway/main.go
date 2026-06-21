package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jruszo/datamingle/gateway/internal/auth"
	"github.com/jruszo/datamingle/gateway/internal/cache"
	"github.com/jruszo/datamingle/gateway/internal/proxy"
)

const (
	defaultVictoriaMetricsURL = "http://victoriametrics-local-dev:8428"
	defaultRedisURL           = "redis://redis:6379"
	defaultGatewayPort        = "4430"

	prometheusRemoteWritePath = "/api/v1/write"
	otlpMetricsPath           = "/opentelemetry/v1/metrics"
)

func main() {
	rawDefaultMetricsURL := strings.TrimSpace(os.Getenv("VICTORIAMETRICS_DEFAULT_URL"))
	defaultMetricsURL := ""
	if rawDefaultMetricsURL == "" {
		defaultMetricsURL = defaultVictoriaMetricsURL
	} else {
		normalizedURL, ok := normalizeBackendURL(rawDefaultMetricsURL)
		if !ok {
			slog.Error("invalid default VictoriaMetrics URL", "url", rawDefaultMetricsURL)
			os.Exit(1)
		}
		defaultMetricsURL = normalizedURL
	}

	tenantMetricsURLs := parseTenantURLs(os.Getenv("VICTORIAMETRICS_TENANT_URLS"))

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

	metricsBackends := proxy.NewTenantBackendResolver(defaultMetricsURL, tenantMetricsURLs)
	promWriteProxy := proxy.MetricsHandler(metricsBackends, prometheusRemoteWritePath)
	otlpMetricsProxy := proxy.MetricsHandler(metricsBackends, otlpMetricsPath)

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
		"default_victoriametrics_url", defaultMetricsURL,
		"tenant_url_count", len(tenantMetricsURLs),
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

func parseTenantURLs(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	var parsed map[string]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		slog.Warn("ignoring invalid VICTORIAMETRICS_TENANT_URLS JSON", "error", err)
		return nil
	}

	cleaned := make(map[string]string, len(parsed))
	for tenantID, backendURL := range parsed {
		tenantID = strings.TrimSpace(tenantID)
		backendURL, ok := normalizeBackendURL(backendURL)
		if tenantID == "" || !ok {
			slog.Warn("ignoring invalid tenant VictoriaMetrics URL", "tenant_id", tenantID)
			continue
		}
		cleaned[tenantID] = backendURL
	}
	return cleaned
}

func normalizeBackendURL(raw string) (string, bool) {
	backendURL := strings.TrimRight(strings.TrimSpace(raw), "/")
	if backendURL == "" {
		return "", false
	}
	parsedURL, err := url.Parse(backendURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return "", false
	}
	return backendURL, true
}
