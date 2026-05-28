package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/jruszo/datamingle/gateway/internal/auth"
)

func CortexHandler(targetURL string) http.Handler {
	target, err := url.Parse(targetURL)
	if err != nil {
		panic("invalid cortex target URL: " + err.Error())
	}

	reverseProxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)

			orgID, ok := auth.OrgIDFromContext(pr.In.Context())
			if !ok {
				slog.Error("missing org_id in context, auth middleware should have set this")
				return
			}

			pr.Out.Header.Set("X-Scope-OrgID", orgID)

			slog.Info("proxying request",
				"method", pr.In.Method,
				"path", pr.In.URL.Path,
				"target", target.String(),
				"org_id", orgID,
			)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Error("proxy error", "error", err, "path", r.URL.Path)
			http.Error(w, `{"error":"gateway proxy error"}`, http.StatusBadGateway)
		},
	}

	return RequireOrgID(reverseProxy)
}

func RequireOrgID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := auth.OrgIDFromContext(r.Context()); !ok {
			slog.Error("missing org_id in request context, refusing to proxy")
			http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
			return
		}
		next.ServeHTTP(w, r)
	})
}
