package walg

import (
	"archive/tar"
	"github.com/pkg/errors"
	"io"
)

// A TarBall represents one tar file.
type TarBall interface {
	SetUp(crypter Crypter, args ...string)
	CloseTar() error
	Finish(sentinelDto *S3TarBallSentinelDto) error
	GetFileRelPath(fileAbsPath string) string
	Size() int64
	AddSize(int64)
	TarWriter() *tar.Writer
	FileExtension() string
	AwaitUploads()
}

func PackFileTo(tarBall TarBall, fileInfoHeader *tar.Header, fileContent io.Reader) (fileSize int64, err error) {
	tarWriter := tarBall.TarWriter()
	err = tarWriter.WriteHeader(fileInfoHeader)
	if err != nil {
		return 0, errors.Wrap(err, "PackFileTo: failed to write header")
	}

	fileSize, err = io.Copy(tarWriter, fileContent)
	if err != nil {
		return fileSize, errors.Wrap(err, "PackFileTo: copy failed")
	}

	tarBall.AddSize(fileInfoHeader.Size)
	return fileSize, err
}
