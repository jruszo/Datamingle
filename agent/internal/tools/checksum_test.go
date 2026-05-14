package tools

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVerifySHA256(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tool")
	if err := os.WriteFile(path, []byte("datamingle"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := VerifySHA256(path, "d459a6c4b0867e9f665a7db35f4387d11fa7fa79a00a85c2c172ba0fa4295c14"); err != nil {
		t.Fatal(err)
	}
	if err := VerifySHA256(path, "bad"); err == nil {
		t.Fatal("expected checksum mismatch")
	}
}
