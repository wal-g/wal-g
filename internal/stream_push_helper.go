package internal

import (
	"io"
	"os"
	"path"

	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const (
	StreamPrefix = "stream_"
)

// TODO : unit tests
// PushStream compresses a stream and push it
func (uploader *Uploader) PushStream(stream io.Reader) (string, error) {
	backupName := StreamPrefix + utility.TimeNowCrossPlatformUTC().Format("20060102T150405Z")
	dstPath := getStreamName(backupName, uploader.Compressor.FileExtension())
	err := uploader.PushStreamToDestination(stream, dstPath)

	return backupName, err
}

// TODO : unit tests
// PushStreamToDestination compresses a stream and push it to specifyed destination
func (uploader *Uploader) PushStreamToDestination(stream io.Reader, dstPath string) error {
	compressed := CompressAndEncrypt(stream, uploader.Compressor, ConfigureCrypter())
	err := uploader.Upload(dstPath, compressed)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)

	return err
}

// FileIsPiped Check if file is piped
func FileIsPiped(stream *os.File) bool {
	stat, _ := stream.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}

func getStreamName(backupName string, extension string) string {
	return utility.SanitizePath(path.Join(backupName, "stream.")) + extension
}
