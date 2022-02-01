package greenplum

import (
	"fmt"
	"path"

	"github.com/wal-g/wal-g/utility"
)

const SegmentsFolderPath = "segments_" + utility.VersionStr + "/"

func FormatSegmentStoragePrefix(contentID int) string {
	segmentFolderName := fmt.Sprintf("seg%d", contentID)
	return path.Join(SegmentsFolderPath, segmentFolderName)
}

func FormatSegmentBackupPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.BaseBackupPath)
}

func FormatSegmentWalPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.WalPath)
}
