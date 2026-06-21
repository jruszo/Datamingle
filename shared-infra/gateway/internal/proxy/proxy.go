package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/jruszo/datamingle/gateway/internal/auth"
)

type TenantBackendResolver struct {
	defaultBaseURL string
	tenantBaseURLs map[string]string
}

func NewTenantBackendResolver(defaultBaseURL string, tenantBaseURLs map[string]string) TenantBackendResolver {
	return TenantBackendResolver{
		defaultBaseURL: defaultBaseURL,
		tenantBaseURLs: tenantBaseURLs,
	}
}

func (r TenantBackendResolver) URLForTenant(tenantID string) string {
	if tenantBaseURL, ok := r.tenantBaseURLs[tenantID]; ok {
		return tenantBaseURL
	}
	return r.defaultBaseURL
}

func MetricsHandler(backends TenantBackendResolver, targetPath string) http.Handler {
	reverseProxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			orgID, ok := auth.OrgIDFromContext(pr.In.Context())
			if !ok {
				slog.Error("missing org_id in context, auth middleware should have set this")
				return
			}

			target, err := url.Parse(backends.URLForTenant(orgID) + targetPath)
			if err != nil {
				slog.Error("invalid metrics backend target URL", "error", err, "org_id", orgID)
				return
			}

			pr.SetURL(target)
			pr.Out.URL.Path = target.Path
			pr.Out.URL.RawPath = target.RawPath
			pr.Out.URL.RawQuery = target.RawQuery

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
