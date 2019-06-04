package mongo

import (
	"io"
	"os"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

func HandleOplogPush(uploader *Uploader) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(OplogPath)
	backupName := OplogPrefix + time.Now().UTC().Format(time.RFC3339)
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
