package walg

import "archive/tar"


// A TarBall represents one tar file.
type TarBall interface {
	SetUp(crypter Crypter, args ...string)
	CloseTar() error
	Finish(sentinelDto *S3TarBallSentinelDto) error
	Trim() string
	Size() int64
	AddSize(int64)
	TarWriter() *tar.Writer
	FileExtension() string
	AwaitUploads()
}