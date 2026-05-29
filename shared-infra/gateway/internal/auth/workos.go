package auth

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jruszo/datamingle/gateway/internal/cache"
	"github.com/workos/workos-go/v9"
)

type contextKey string

const OrgIDKey contextKey = "org_id"

type Authenticator struct {
	workosClient *workos.Client
	apiKeyCache  *cache.APIKeyCache
}

func NewAuthenticator(apiKey string, apiKeyCache *cache.APIKeyCache) *Authenticator {
	return &Authenticator{
		workosClient: workos.NewClient(apiKey),
		apiKeyCache:  apiKeyCache,
	}
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := extractBearerToken(r)
		if err != nil {
			slog.Warn("auth failed", "error", err.Error())
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusUnauthorized)
			return
		}

		orgID, err := a.validate(r.Context(), token)
		if err != nil {
			slog.Warn("api key validation failed", "error", err.Error())
			http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), OrgIDKey, orgID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *Authenticator) validate(ctx context.Context, token string) (string, error) {
	if cachedOrgID, ok := a.apiKeyCache.GetOrgID(ctx, token); ok {
		return cachedOrgID, nil
	}

	resp, err := a.workosClient.APIKeys().CreateValidation(
		ctx,
		&workos.APIKeysCreateValidationParams{
			Value: token,
		},
	)
	if err != nil {
		return "", err
	}

	if resp.APIKey == nil || resp.APIKey.Owner == nil || resp.APIKey.Owner.Type != "organization" {
		return "", errors.New("api key must belong to an organization")
	}

	orgID := resp.APIKey.Owner.ID

	slog.Info("api key validated",
		"api_key_id", resp.APIKey.ID,
		"org_id", orgID,
	)

	a.apiKeyCache.SetOrgID(ctx, token, orgID)

	return orgID, nil
}

func OrgIDFromContext(ctx context.Context) (string, bool) {
	orgID, ok := ctx.Value(OrgIDKey).(string)
	return orgID, ok && orgID != ""
}

var (
	errMissingHeader    = errors.New("missing authorization header")
	errInvalidHeaderFmt = errors.New("invalid authorization header format")
)

func extractBearerToken(r *http.Request) (string, error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", errMissingHeader
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", errInvalidHeaderFmt
	}
	return parts[1], nil
}
