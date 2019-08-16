package mongo

import (
	"os"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleOplogPush(uploader *Uploader) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(OplogPath)
	if !internal.FileIsPiped(os.Stdin) {
		tracelog.ErrorLogger.Fatal("Use stdin\n")
	}
	oplogName := OplogPrefix + utility.TimeNowCrossPlatformUTC().Format("20060102T150405Z")
	dstPath := oplogName + "." + uploader.Compressor.FileExtension()
	err := uploader.PushStreamToDestination(os.Stdin, dstPath)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Oplog file " + dstPath + " was uploaded")
}
