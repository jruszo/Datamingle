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
	path := installIDPath(dataDir)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return LoadInstallID(dataDir)
		}
		return "", err
	}
	defer file.Close()

	installID, err := generateInstallID()
	if err != nil {
		return "", err
	}
	if _, err := file.WriteString(installID + "\n"); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
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
