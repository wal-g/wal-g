package storage

import (
	"fmt"
	"io"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
)

type StreamSentinelDto struct {
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
}

// Uploader extends base uploader with mongodb specific.
type Uploader struct {
	*internal.Uploader
}

// NewUploader builds mongodb uploader.
// TODO: use functional arguments
func NewUploader(path string) (*Uploader, error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return nil, err
	}
	if path != "" {
		uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(path)
	}
	return &Uploader{uploader}, nil
}

// UploadStreamTo compresses a stream and uploads it with given archive name.
// TODO : unit tests
func (uploader *Uploader) UploadStreamTo(stream io.Reader, filename string) error {
	if err := uploader.PushStreamToDestination(stream, filename); err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	tracelog.InfoLogger.Println("File " + filename + " was uploaded")
	return nil
}

// UploadStream compresses a stream and uploads it.
// TODO : unit tests
func (uploader *Uploader) UploadStream(stream io.Reader) error {
	timeStart := utility.TimeNowCrossPlatformLocal()
	backupName, err := uploader.PushStream(stream)
	if err != nil {
		return err
	}
	currentBackupSentinelDto := &StreamSentinelDto{
		StartLocalTime:  timeStart,
		FinishLocalTime: utility.TimeNowCrossPlatformLocal(),
		UserData:        internal.GetSentinelUserData(),
	}
	return internal.UploadSentinel(uploader.Uploader, currentBackupSentinelDto, backupName)
}
