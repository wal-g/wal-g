package mongo

import (
	"path"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const (
	OplogPrefix = "oplog_"
	OplogPath   = "oplog_" + utility.VersionStr + "/"
	OplogEndTs  = "WALG_MONGO_OPLOG_END_TS"
	OplogDst    = "WALG_MONGO_OPLOG_DST"
)

func getStreamName(backupName string, extension string) string {
	return utility.SanitizePath(path.Join(backupName, "stream.")) + extension
}

type Uploader struct {
	*internal.Uploader
}

type StreamSentinelDto struct {
	StartLocalTime time.Time
}
