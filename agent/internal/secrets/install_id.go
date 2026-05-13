package secrets

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const installIDFileName = "install_id"

func LoadOrCreateInstallID(dataDir string) (string, error) {
	if installID, err := LoadInstallID(dataDir); err == nil {
		return installID, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return "", err
	}
	installID, err := generateInstallID()
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(installIDPath(dataDir), []byte(installID+"\n"), 0o600); err != nil {
		return "", err
	}
	return installID, nil
}

func LoadInstallID(dataDir string) (string, error) {
	value, err := os.ReadFile(installIDPath(dataDir))
	if err != nil {
		return "", err
	}
	installID := strings.TrimSpace(string(value))
	if installID == "" {
		return "", fmt.Errorf("install ID file is empty")
	}
	return installID, nil
}

func installIDPath(dataDir string) string {
	return filepath.Join(dataDir, installIDFileName)
}

func generateInstallID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "ins_" + hex.EncodeToString(raw[:]), nil
}
