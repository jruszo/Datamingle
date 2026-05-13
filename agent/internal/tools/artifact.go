package tools

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type Artifact struct {
	ToolName     string
	Version      string
	Platform     string
	Architecture string
	DownloadURL  string
	SHA256       string
	SizeBytes    int64
}

func EnsureArtifact(ctx context.Context, cacheDir string, artifact Artifact, httpClient *http.Client) (string, error) {
	if strings.TrimSpace(cacheDir) == "" {
		return "", fmt.Errorf("tool cache directory is required")
	}
	if err := validateArtifact(artifact); err != nil {
		return "", err
	}
	path := ArtifactPath(cacheDir, artifact)
	if err := VerifySHA256(path, artifact.SHA256); err == nil {
		return path, nil
	}
	_ = os.Remove(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".download-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	client := httpClient
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifact.DownloadURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("download %s failed: %s", artifact.DownloadURL, resp.Status)
	}
	reader := io.Reader(resp.Body)
	if artifact.SizeBytes > 0 {
		reader = io.LimitReader(resp.Body, artifact.SizeBytes+1)
	}
	written, err := io.Copy(tmp, reader)
	if err != nil {
		return "", err
	}
	if artifact.SizeBytes > 0 && written > artifact.SizeBytes {
		return "", fmt.Errorf("downloaded artifact exceeded expected size %d", artifact.SizeBytes)
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := VerifySHA256(tmpPath, artifact.SHA256); err != nil {
		return "", err
	}
	if err := os.Chmod(tmpPath, 0o755); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return "", err
	}
	return path, nil
}

func ArtifactPath(cacheDir string, artifact Artifact) string {
	return filepath.Join(
		cacheDir,
		safePathComponent(artifact.ToolName),
		safePathComponent(artifact.Version),
		safePathComponent(artifact.Platform+"-"+artifact.Architecture),
		safePathComponent(artifact.ToolName),
	)
}

func validateArtifact(artifact Artifact) error {
	if strings.TrimSpace(artifact.ToolName) == "" {
		return fmt.Errorf("tool name is required")
	}
	if strings.TrimSpace(artifact.Version) == "" {
		return fmt.Errorf("tool version is required")
	}
	if strings.TrimSpace(artifact.Platform) == "" || strings.TrimSpace(artifact.Architecture) == "" {
		return fmt.Errorf("tool platform and architecture are required")
	}
	if strings.TrimSpace(artifact.DownloadURL) == "" {
		return fmt.Errorf("tool download URL is required")
	}
	if strings.TrimSpace(artifact.SHA256) == "" {
		return fmt.Errorf("tool SHA256 is required")
	}
	return nil
}

func safePathComponent(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", "..", "_")
	return replacer.Replace(strings.TrimSpace(value))
}
