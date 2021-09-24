package greenplum

import (
	"fmt"
	"path"

	"github.com/wal-g/wal-g/utility"
)

func FormatSegmentStoragePrefix(contentID int) string {
	return fmt.Sprintf("seg%d", contentID)
}

func FormatSegmentBackupPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.BaseBackupPath)
}

func FormatSegmentWalPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.WalPath)
}
