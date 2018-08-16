package testtools

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"bytes"
	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g"
)

// FileTarBall represents a tarball that is
// written to disk.
type FileTarBall struct {
	out         string
	number      int
	size        int64
	writeCloser io.WriteCloser
	tarWriter   *tar.Writer
}

// SetUp creates a new LZ4 writer, tar writer and file for
// writing bundled compressed bytes to.
func (tarBall *FileTarBall) SetUp(crypter walg.Crypter, names ...string) {
	if tarBall.tarWriter == nil {
		name := filepath.Join(tarBall.out, "part_"+fmt.Sprintf("%0.3d", tarBall.number)+".tar.lz4")
		file, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		var writeCloser io.WriteCloser

		if crypter.IsUsed() {
			writeCloser, err = crypter.Encrypt(file)

			if err != nil {
				panic(err)
			}

			tarBall.writeCloser = &walg.CascadeWriteCloser{
				WriteCloser: lz4.NewWriter(file),
				Underlying:  &walg.CascadeWriteCloser{WriteCloser: writeCloser, Underlying: file},
			}
		} else {
			writeCloser = file
			tarBall.writeCloser = &walg.CascadeWriteCloser{
				WriteCloser: lz4.NewWriter(file),
				Underlying:  writeCloser,
			}
		}

		tarBall.tarWriter = tar.NewWriter(tarBall.writeCloser)
	}
}

// CloseTar closes the tar writer and file, flushing any
// unwritten data to the file before closing.
func (tarBall *FileTarBall) CloseTar() error {
	err := tarBall.tarWriter.Close()
	if err != nil {
		return err
	}

	return tarBall.writeCloser.Close()
}

// Finish alerts that compression is complete.
func (tarBall *FileTarBall) Finish(sentinelDto *walg.S3TarBallSentinelDto) error {
	fmt.Printf("Wrote %d compressed tar files to %s.\n", tarBall.number, tarBall.out)
	return nil
}

func (tarBall *FileTarBall) Size() int64            { return tarBall.size }
func (tarBall *FileTarBall) AddSize(i int64)        { tarBall.size += i }
func (tarBall *FileTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
func (tarBall *FileTarBall) AwaitUploads()          {}

// NOPTarBall mocks a tarball. Used for testing purposes.
type NOPTarBall struct {
	number    int
	size      int64
	tarWriter *tar.Writer
}

func (tarBall *NOPTarBall) SetUp(crypter walg.Crypter, params ...string) {}
func (tarBall *NOPTarBall) CloseTar() error                              { return nil }
func (tarBall *NOPTarBall) Finish(sentinelDto *walg.S3TarBallSentinelDto) error {
	fmt.Printf("NOP: %d files.\n", tarBall.number)
	return nil
}

func (tarBall *NOPTarBall) Size() int64            { return tarBall.size }
func (tarBall *NOPTarBall) AddSize(i int64)        { tarBall.size += i }
func (tarBall *NOPTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
func (tarBall *NOPTarBall) AwaitUploads()          {}

// BufferTarBall represents a tarball that is
// written to buffer.
type BufferTarBall struct {
	number     int
	size       int64
	underlying *bytes.Buffer
	tarWriter  *tar.Writer
}

func (tarBall *BufferTarBall) SetUp(crypter walg.Crypter, args ...string) {
	tarBall.tarWriter = tar.NewWriter(tarBall.underlying)
}

func (tarBall *BufferTarBall) CloseTar() error {
	return tarBall.tarWriter.Close()
}

func (tarBall *BufferTarBall) Finish(sentinelDto *walg.S3TarBallSentinelDto) error {
	return nil
}

func (tarBall *BufferTarBall) Size() int64 {
	return tarBall.size
}

func (tarBall *BufferTarBall) AddSize(add int64) {
	tarBall.size += add
}

func (tarBall *BufferTarBall) TarWriter() *tar.Writer {
	return tarBall.tarWriter
}

func (tarBall *BufferTarBall) AwaitUploads() {}
