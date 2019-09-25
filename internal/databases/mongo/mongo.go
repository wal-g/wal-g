package mongo

import (
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

type Uploader struct {
	*internal.Uploader
}

type StreamSentinelDto struct {
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
}
