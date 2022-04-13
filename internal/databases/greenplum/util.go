package greenplum

import (
	"fmt"
	"path"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/utility"
)

const SegmentsFolderPath = "segments_" + utility.VersionStr + "/"

func FormatSegmentStoragePrefix(contentID int) string {
	segmentFolderName := fmt.Sprintf("seg%d", contentID)
	return path.Join(SegmentsFolderPath, segmentFolderName)
}

func formatSegmentLogPath(contentID int) string {
	logsDir := viper.GetString(internal.GPLogsDirectory)
	return fmt.Sprintf("%s/%s-seg%d.log", logsDir, SegBackupLogPrefix, contentID)
}

func FormatSegmentBackupPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.BaseBackupPath)
}

func FormatSegmentWalPath(contentID int) string {
	return path.Join(FormatSegmentStoragePrefix(contentID), utility.WalPath)
}
