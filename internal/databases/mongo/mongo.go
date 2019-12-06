package mongo

import (
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
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

type OplogArchName struct {
	startTS string
	ext     string
}

func GetOplogArchPath(startTS, endTS OplogTimestamp, ext string) string {
	// oplog_1569009857.10_1569009101.99.lzma
	return fmt.Sprintf("%s%v%s%v.%s", OplogArchNamePrefix, startTS, OplogArchNameTSDelimiter, endTS, ext)
}

func GetOplogEndTS(path string) (string, error) {
	reStr := fmt.Sprintf(`%s(?P<startTS>%s)%s(?P<endTS>%s)\.`,
		OplogArchNamePrefix, OplogTimestampRegexp, OplogArchNameTSDelimiter, OplogTimestampRegexp)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return "", fmt.Errorf("can not compile oplog archive name regexp: %w", err)
	}
	res := re.FindAllStringSubmatch(path, -1)
	for i := range res {
		return res[i][2], nil
	}
	return "", fmt.Errorf("can not parse oplog path: %s", path)
}

type Uploader struct {
	*internal.Uploader
}

func (uploader *Uploader) uploadOplogStream(stream io.Reader, startTS, endTS OplogTimestamp) error {
	dstPath := GetOplogArchPath(startTS, endTS, uploader.Compressor.FileExtension())
	err := uploader.PushStreamToDestination(stream, dstPath)
	if err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	tracelog.InfoLogger.Println("Oplog file " + dstPath + " was uploaded")
	return nil
}
