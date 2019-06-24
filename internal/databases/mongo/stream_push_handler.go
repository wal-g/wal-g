package mongo

import (
	"bytes"
	"encoding/json"
	"io"
	"os"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamPush(uploader *Uploader) {
	if !internal.FileIsPiped(os.Stdin) {
		tracelog.ErrorLogger.Fatal("Use stdin\n")
	}
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	err := uploader.UploadStream(os.Stdin)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
// UploadStream compresses a stream and uploads it.
func (uploader *Uploader) UploadStream(stream io.Reader) error {
	timeStart := utility.TimeNowCrossPlatformLocal()
	backupName, err := uploader.PushStream(stream)
	uploadStreamSentinel(&StreamSentinelDto{StartLocalTime: timeStart}, uploader, backupName+utility.SentinelSuffix)
	return err
}

func uploadStreamSentinel(sentinelDto *StreamSentinelDto, uploader *Uploader, name string) error {
	dtoBody, err := json.Marshal(*sentinelDto)
	if err != nil {
		return err
	}

	uploadingErr := uploader.Upload(name, bytes.NewReader(dtoBody))
	if uploadingErr != nil {
		tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", name)
		tracelog.ErrorLogger.Fatalf("StorageTarBall finish: json failed to upload")
		return uploadingErr
	}
	return nil
}
