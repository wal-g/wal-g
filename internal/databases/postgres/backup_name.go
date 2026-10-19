package postgres

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// newBackupAttemptName separates the physical backup identity from its recovery
// position. Standbys can start different backups at the same restartpoint, even
// after replaying changes. Neither an LSN nor a wall-clock timestamp identifies
// an attempt uniquely across hosts.
// Generate this name once, before uploading any objects, and reuse it for the
// data, metadata and completion sentinel. A restarted backup gets a new name.
func newBackupAttemptName(baseName string) (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate backup attempt ID: %w", err)
	}
	return baseName + "_U_" + hex.EncodeToString(id[:]), nil
}
