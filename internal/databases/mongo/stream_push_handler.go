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
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	stat, _ := os.Stdin.Stat()
	var stream io.Reader = os.Stdin
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		tracelog.InfoLogger.Println("Data is piped from stdin")
	} else {
		tracelog.ErrorLogger.Fatal("Use stdin\n")
	}
	err := uploader.UploadStream(stream)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
// UploadStream compresses a stream and uploads it.
func (uploader *Uploader) UploadStream(stream io.Reader) error {
	timeStart := utility.TimeNowCrossPlatformLocal()
	compressor := uploader.Compressor

	compressed := internal.CompressAndEncrypt(stream, compressor, internal.ConfigureCrypter())

	backupName := StreamPrefix + utility.TimeNowCrossPlatformUTC().Format("20060102T150405Z")
	dstPath := getStreamName(backupName, compressor.FileExtension())
	tracelog.DebugLogger.Println("Upload path", dstPath)

	err := uploader.Upload(dstPath, compressed)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)

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
