package mongo

import (
	"encoding/json"
	"io/ioutil"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const (
	StreamPrefix = "stream_"
	OplogPrefix  = "oplog_"
	OplogPath    = "oplog_" + utility.VersionStr + "/"
	OplogEndTs   = "WALG_MONGO_OPLOG_END_TS"
	OplogDst     = "WALG_MONGO_OPLOG_DST"
)

type Uploader struct {
	*internal.Uploader
}
type Backup struct {
	*internal.Backup
}

func getStreamName(backupName string, extension string) string {
	return utility.SanitizePath(path.Join(backupName, "stream.")) + extension
}

// TODO : unit tests
func (backup *Backup) FetchStreamSentinel() (StreamSentinelDto, error) {
	sentinelDto := StreamSentinelDto{}
	backupReaderMaker := internal.NewStorageReaderMaker(backup.BaseBackupFolder,
		backup.GetStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return sentinelDto, err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
}

type StreamSentinelDto struct {
	StartLocalTime time.Time
}
