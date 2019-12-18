package mongo

import (
	"fmt"
	"io"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
)

type StreamSentinelDto struct {
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
}

type Uploader struct {
	*internal.Uploader
}

func (uploader *Uploader) ArchiveUpload(stream io.Reader, start, end oplog.Timestamp) error {
	arch, err := oplog.NewArchive(start, end, uploader.Compressor.FileExtension())
	if err != nil {
		return err
	}
	filename := arch.Filename()
	if err := uploader.PushStreamToDestination(stream, filename); err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	tracelog.InfoLogger.Println("Archive file " + filename + " was uploaded")
	return nil
}
