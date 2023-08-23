package postgres

import (
	"github.com/wal-g/wal-g/internal"
)

// BackupDetail is used to append ExtendedMetadataDto details to BackupTime struct
type BackupDetail struct {
	internal.BackupTime
	ExtendedMetadataDto
}
