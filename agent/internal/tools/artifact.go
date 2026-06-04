package tools

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const maxArtifactDownloadBytes int64 = 512 * 1024 * 1024

var (
	safePathComponentPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	whitespacePattern        = regexp.MustCompile(`\s+`)
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
	artifact = sanitizeArtifact(artifact)
	if err := validateArtifact(artifact); err != nil {
		return "", err
	}
	path := ArtifactPath(cacheDir, artifact)
	if cachedArtifactMatches(path, artifact.SHA256) {
		return path, nil
	}
	_ = os.Remove(path)
	_ = os.Remove(artifactChecksumPath(path))
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
	limit := maxArtifactDownloadBytes + 1
	if artifact.SizeBytes > 0 && artifact.SizeBytes < maxArtifactDownloadBytes {
		limit = artifact.SizeBytes + 1
	}
	reader := io.LimitReader(resp.Body, limit)
	written, err := io.Copy(tmp, reader)
	if err != nil {
		return "", err
	}
	if written > maxArtifactDownloadBytes {
		return "", fmt.Errorf("downloaded artifact exceeded max size %d", maxArtifactDownloadBytes)
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

	if strings.HasSuffix(strings.ToLower(artifact.DownloadURL), ".tar.gz") || strings.HasSuffix(strings.ToLower(artifact.DownloadURL), ".tgz") {
		if err := extractArtifactBinary(tmpPath, path, artifact.ToolName); err != nil {
			return "", err
		}
	} else {
		if err := os.Chmod(tmpPath, 0o755); err != nil {
			return "", err
		}
		if err := os.Rename(tmpPath, path); err != nil {
			return "", err
		}
	}
	if err := os.WriteFile(artifactChecksumPath(path), []byte(artifact.SHA256), 0o644); err != nil {
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

func sanitizeArtifact(artifact Artifact) Artifact {
	artifact.ToolName = sanitizeArtifactValue(artifact.ToolName)
	artifact.Version = sanitizeArtifactValue(artifact.Version)
	artifact.Platform = sanitizeArtifactValue(artifact.Platform)
	artifact.Architecture = sanitizeArtifactValue(artifact.Architecture)
	artifact.DownloadURL = sanitizeArtifactValue(artifact.DownloadURL)
	artifact.SHA256 = sanitizeArtifactValue(artifact.SHA256)
	return artifact
}

func sanitizeArtifactValue(value string) string {
	value = strings.ReplaceAll(value, "\x00", "")
	value = normalizePathComponent(value)
	return strings.TrimSpace(value)
}

func safePathComponent(value string) string {
	value = sanitizeArtifactValue(value)
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		"..", "_",
	)
	result := replacer.Replace(value)
	result = whitespacePattern.ReplaceAllString(strings.TrimSpace(result), "_")
	if result == "" || result == "." || result == ".." {
		return "_"
	}
	if !safePathComponentPattern.MatchString(result) {
		return "_"
	}
	return result
}

func normalizePathComponent(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= '！' && r <= '～' {
			return r - 0xFEE0
		}
		if r == '　' {
			return ' '
		}
		return r
	}, value)
}

func cachedArtifactMatches(path, expectedSHA256 string) bool {
	if err := VerifySHA256(path, expectedSHA256); err == nil {
		return true
	}
	raw, err := os.ReadFile(artifactChecksumPath(path))
	if err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(string(raw)), strings.TrimSpace(expectedSHA256)) {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func artifactChecksumPath(path string) string {
	return path + ".sha256"
}

func extractArtifactBinary(archivePath, targetPath, toolName string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("open gzip artifact: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	tmp, err := os.CreateTemp(filepath.Dir(targetPath), ".extract-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar artifact: %w", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if filepath.Base(header.Name) != toolName {
			continue
		}
		if _, err := io.Copy(tmp, tarReader); err != nil {
			return err
		}
		if err := tmp.Close(); err != nil {
			return err
		}
		if err := os.Chmod(tmpPath, 0o755); err != nil {
			return err
		}
		if err := os.Rename(tmpPath, targetPath); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("artifact archive did not contain %s", toolName)
}
