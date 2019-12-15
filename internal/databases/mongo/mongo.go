package mongo

import (
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/utility"
)

const (
	OplogArchNamePrefix      = "oplog_"
	OplogPath                = "oplog_" + utility.VersionStr + "/"
	OplogArchNameTSDelimiter = "_"
	OplogEndTs               = "WALG_MONGO_OPLOG_END_TS"
	OplogDst                 = "WALG_MONGO_OPLOG_DST"
)

type StreamSentinelDto struct {
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
}

// GetOplogArchName builds archive name from timestamps and extension
// example: oplog_1569009857.10_1569009101.99.lzma
func GetOplogArchName(startTS, endTS oplog.Timestamp, ext string) string {
	return fmt.Sprintf("%s%v%s%v.%s", OplogArchNamePrefix, startTS, OplogArchNameTSDelimiter, endTS, ext)
}

// GetOplogArchTimestamps extracts timestamps from archive name
func GetOplogArchTimestamps(path string) (oplog.Timestamp, oplog.Timestamp, error) {
	// TODO: add unit test and move regexp to const
	reStr := fmt.Sprintf(`%s(?P<startTS>%s)%s(?P<endTS>%s)\.`,
		OplogArchNamePrefix, oplog.TimestampRegexp, OplogArchNameTSDelimiter, oplog.TimestampRegexp)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return oplog.Timestamp{}, oplog.Timestamp{}, fmt.Errorf("can not compile oplog archive name regexp: %w", err)
	}
	res := re.FindAllStringSubmatch(path, -1)
	for i := range res {
		startTS, startErr := oplog.TimestampFromStr(res[i][1])
		endTS, endErr := oplog.TimestampFromStr(res[i][2])
		if startErr != nil || endErr != nil {
			break
		}
		return startTS, endTS, nil
	}
	return oplog.Timestamp{}, oplog.Timestamp{}, fmt.Errorf("can not parse oplog path: %s", path)
}

// DiscoveryArchiveResumeTS returns archiving start timestamp
func DiscoveryArchiveResumeTS(folder storage.Folder) (oplog.Timestamp, bool, error) {
	lastKnownTS, err := LastKnownArchiveTS(folder)
	if err != nil {
		return oplog.Timestamp{}, false, err
	}
	zeroTS := oplog.Timestamp{}
	if lastKnownTS == zeroTS {
		// TODO: add additional check
		return zeroTS, true, nil
	}
	return lastKnownTS, false, nil
}

// LastKnownArchiveTS returns the most recent existed timestamp in storage folder
func LastKnownArchiveTS(folder storage.Folder) (oplog.Timestamp, error) {
	maxTS := oplog.Timestamp{}
	oplogArchives, _, err := folder.ListFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	for _, arch := range oplogArchives {
		_, endTS, err := GetOplogArchTimestamps(arch.GetName())
		if err != nil {
			return oplog.Timestamp{}, fmt.Errorf("can not convert retrieve timestamps from oplog archive name '%s': %w", arch.GetName(), err)
		}
		maxTS = oplog.Max(maxTS, endTS)
	}
	return maxTS, nil
}

type Uploader struct {
	*internal.Uploader
}

func (uploader *Uploader) uploadOplogStream(stream io.Reader, startTS, endTS oplog.Timestamp) error {
	dstPath := GetOplogArchName(startTS, endTS, uploader.Compressor.FileExtension())
	err := uploader.PushStreamToDestination(stream, dstPath)
	if err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	tracelog.InfoLogger.Println("Oplog file " + dstPath + " was uploaded")
	return nil
}
