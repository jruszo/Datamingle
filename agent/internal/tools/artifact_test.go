package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

func TestEnsureArtifactSanitizesArtifactInputs(t *testing.T) {
	body := []byte("online schema tool")
	sum := sha256.Sum256(body)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer server.Close()

	cacheDir := filepath.Join(t.TempDir(), "tools")
	path, err := EnsureArtifact(context.Background(), cacheDir, Artifact{
		ToolName:     " gh／ost\x00 ",
		Version:      " 1.1.6 ",
		Platform:     " linux ",
		Architecture: " amd64 ",
		DownloadURL:  " " + server.URL + "\x00 ",
		SHA256:       " \x00" + hex.EncodeToString(sum[:]) + "\n",
		SizeBytes:    int64(len(body)),
	}, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	expectedSuffix := filepath.Join("gh_ost", "1.1.6", "linux-amd64", "gh_ost")
	if !strings.HasSuffix(path, expectedSuffix) {
		t.Fatalf("expected sanitized artifact path suffix %q, got %q", expectedSuffix, path)
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

func TestEnsureArtifactRejectsDeclaredSizeOverflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("larger than expected"))
	}))
	defer server.Close()

	_, err := EnsureArtifact(context.Background(), t.TempDir(), Artifact{
		ToolName:     "gh-ost",
		Version:      "1.1.6",
		Platform:     "linux",
		Architecture: "amd64",
		DownloadURL:  server.URL,
		SHA256:       "d459a6c4b0867e9f665a7db35f4387d11fa7fa79a00a85c2c172ba0fa4295c14",
		SizeBytes:    3,
	}, server.Client())

	if err == nil || !strings.Contains(err.Error(), "exceeded expected size") {
		t.Fatalf("expected size overflow error, got %v", err)
	}
}

func TestSafePathComponentSanitizesTraversalAndSeparators(t *testing.T) {
	tests := map[string]string{
		" gh-ost ":      "gh-ost",
		"linux amd64":   "linux_amd64",
		"..":            "_",
		".":             "_",
		"a/b\\c":        "a_b_c",
		"a／b＼c":         "a_b_c",
		"bad\x00name":   "badname",
		"bad:name":      "_",
		"../secret":     "__secret",
		"version-1.2.3": "version-1.2.3",
		"two\tspaces":   "two_spaces",
	}

	for input, expected := range tests {
		if got := safePathComponent(input); got != expected {
			t.Fatalf("safePathComponent(%q) = %q, expected %q", input, got, expected)
		}
	}
}
