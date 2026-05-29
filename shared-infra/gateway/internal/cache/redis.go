package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultTTL          = 5 * time.Minute
	apiKeyCacheKeyPrefix = "apikey:"
)

type APIKeyCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewAPIKeyCache(redisURL string, ttl time.Duration) (*APIKeyCache, error) {
	if ttl <= 0 {
		ttl = defaultTTL
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		slog.Warn("redis unavailable, caching disabled", "error", err)
		return &APIKeyCache{client: nil, ttl: ttl}, nil
	}

	slog.Info("redis connected", "url", redisURL, "ttl", ttl)
	return &APIKeyCache{client: client, ttl: ttl}, nil
}

func (c *APIKeyCache) GetOrgID(ctx context.Context, apiKey string) (string, bool) {
	if c.client == nil {
		return "", false
	}

	key := cacheKey(apiKey)
	orgID, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			slog.Warn("redis get failed", "error", err)
		}
		return "", false
	}

	slog.Debug("api key cache hit", "cache_key", key)
	return orgID, true
}

func (c *APIKeyCache) SetOrgID(ctx context.Context, apiKey, orgID string) {
	if c.client == nil {
		return
	}

	key := cacheKey(apiKey)
	if err := c.client.Set(ctx, key, orgID, c.ttl).Err(); err != nil {
		slog.Warn("redis set failed", "error", err)
	}
}

func cacheKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return apiKeyCacheKeyPrefix + hex.EncodeToString(h[:16])
}
