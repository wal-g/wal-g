package mongo

import (
	"io"
	"os"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleOplogPush(uploader *Uploader) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(OplogPath)
	backupName := OplogPrefix + utility.TimeNowCrossPlatformUTC().Format("20060102T150405Z")
	stat, _ := os.Stdin.Stat()
	var stream io.Reader = os.Stdin
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		tracelog.InfoLogger.Println("Data is piped from stdin")
	} else {
		tracelog.ErrorLogger.Fatal("Use stdin\n")
	}
	err := uploader.UploadOplogStream(backupName, stream)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
// UploadOplogStream compresses a stream and upload it as oplog.
func (uploader *Uploader) UploadOplogStream(fileName string, stream io.Reader) error {
	compressed := internal.CompressAndEncrypt(stream, uploader.Compressor, internal.ConfigureCrypter())

	dstPath := fileName + "." + uploader.Compressor.FileExtension()

	err := uploader.Upload(dstPath, compressed)

	tracelog.InfoLogger.Println("Oplog file " + dstPath + " was uploaded")

	return err
}
