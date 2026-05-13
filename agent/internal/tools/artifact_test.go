package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureArtifactDownloadsVerifiesAndCaches(t *testing.T) {
	body := []byte("online schema tool")
	sum := sha256.Sum256(body)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write(body)
	}))
	defer server.Close()

	artifact := Artifact{
		ToolName:     "gh-ost",
		Version:      "1.1.6",
		Platform:     "linux",
		Architecture: "amd64",
		DownloadURL:  server.URL,
		SHA256:       hex.EncodeToString(sum[:]),
		SizeBytes:    int64(len(body)),
	}
	cacheDir := filepath.Join(t.TempDir(), "tools")
	path, err := EnsureArtifact(context.Background(), cacheDir, artifact, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("expected one download, got %d", requests)
	}

	secondPath, err := EnsureArtifact(context.Background(), cacheDir, artifact, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if secondPath != path {
		t.Fatalf("expected cached path %s, got %s", path, secondPath)
	}
	if requests != 1 {
		t.Fatalf("expected cache hit without download, got %d downloads", requests)
	}
}

func TestEnsureArtifactRejectsChecksumMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("tampered"))
	}))
	defer server.Close()

	_, err := EnsureArtifact(context.Background(), t.TempDir(), Artifact{
		ToolName:     "pt-online-schema-change",
		Version:      "3.6.0",
		Platform:     "linux",
		Architecture: "amd64",
		DownloadURL:  server.URL,
		SHA256:       "d459a6c4b0867e9f665a7db35f4387d11fa7fa79a00a85c2c172ba0fa4295c14",
	}, server.Client())
	if err == nil {
		t.Fatal("expected checksum mismatch")
	}
}
