package postgres

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const (
	// Length of the WAL file name part
	WALFileNameLength = 24

	// BackupHistoryFileSuffix represents the file extension for backup history files
	BackupHistoryFileSuffix = ".backup"

	// Minimum length of a backup history filename
	// Format: 000000010000000000000003.00000028.backup
	// - First 24 characters: WAL file name (000000010000000000000003)
	// - 1 character separator (.)
	// - 8 character hexadecimal offset (00000028)
	// - 7 character suffix (.backup)
	MinBackupHistoryLength = WALFileNameLength + 1 + 8 + len(BackupHistoryFileSuffix)
)

// ParseBackupHistoryFilename extracts numeric parts from backup history filename
func ParseBackupHistoryFilename(name string) (timelineID uint32, logSegNo uint64, offset uint32, err error) {
	if len(name) < MinBackupHistoryLength || !strings.HasSuffix(name, BackupHistoryFileSuffix) {
		err = newNotBackupHistoryFilenameError(name)
		return
	}

	// Parse timeline and logSegNo using existing ParseWALFilename
	timelineID, logSegNo, err = ParseWALFilename(name[:WALFileNameLength])
	if err != nil {
		return
	}

	// Parse the offset part
	offsetStr := name[WALFileNameLength+1 : len(name)-len(BackupHistoryFileSuffix)]
	offset64, err := strconv.ParseUint(offsetStr, 0x10, sizeofInt32bits)
	if err != nil {
		return
	}
	offset = uint32(offset64)

	return
}

// IsBackupHistoryFilename checks if the given filename is a backup history file
func IsBackupHistoryFilename(filename string) bool {
	_, _, _, err := ParseBackupHistoryFilename(filename)
	return err == nil
}

// GetWalFilenameFromBackupHistoryFilename extracts the WAL file name from a backup history file name
func GetWalFilenameFromBackupHistoryFilename(backupHistoryFilename string) (string, error) {
	timelineID, logSegNo, _, err := ParseBackupHistoryFilename(backupHistoryFilename)
	if err != nil {
		return "", err
	}

	walFilename := formatWALFileName(timelineID, logSegNo)

	return walFilename, nil
}

type NotBackupHistoryFilenameError struct {
	error
}

func newNotBackupHistoryFilenameError(filename string) NotBackupHistoryFilenameError {
	return NotBackupHistoryFilenameError{errors.Errorf("not a backup history file name: %s", filename)}
}

func (err NotBackupHistoryFilenameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}
