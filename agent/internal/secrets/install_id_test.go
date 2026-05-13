package secrets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadOrCreateInstallIDPersistsValue(t *testing.T) {
	dataDir := t.TempDir()
	first, err := LoadOrCreateInstallID(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreateInstallID(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("expected persisted install ID, got %q and %q", first, second)
	}
	if !strings.HasPrefix(first, "ins_") {
		t.Fatalf("unexpected install ID prefix: %q", first)
	}

	info, err := os.Stat(filepath.Join(dataDir, installIDFileName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected 0600 permissions, got %v", info.Mode().Perm())
	}
}
