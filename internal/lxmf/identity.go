package lxmf

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/thatSFguy/reticulum-go/rns"
)

// LoadOrCreateIdentity loads an existing Reticulum identity file or creates and saves a new one.
func LoadOrCreateIdentity(path string) (*rns.Identity, error) {
	if _, err := os.Stat(path); err == nil {
		id, err := rns.IdentityFromFile(path)
		if err != nil {
			return nil, fmt.Errorf("load identity from %s: %w", path, err)
		}
		return id, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat identity file %s: %w", path, err)
	}

	// Create new identity
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create identity directory: %w", err)
	}

	id, err := rns.NewIdentity()
	if err != nil {
		return nil, fmt.Errorf("generate identity: %w", err)
	}

	if err := id.Save(path); err != nil {
		return nil, fmt.Errorf("save identity to %s: %w", path, err)
	}

	return id, nil
}
