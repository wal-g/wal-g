package testtools

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
)

// FileTarBall represents a tarball that is
// written to disk.
type FileTarBall struct {
	name        string
	out         string
	number      int
	size        int64
	writeCloser io.WriteCloser
	tarWriter   *tar.Writer
}

func (tarBall *FileTarBall) Name() string { return tarBall.name }

// SetUp creates a new LZ4 writer, tar writer and file for
// writing bundled compressed bytes to.
func (tarBall *FileTarBall) SetUp(crypter crypto.Crypter, names ...string) {
	if tarBall.tarWriter == nil {
		name := filepath.Join(tarBall.out, "part_"+fmt.Sprintf("%0.3d", tarBall.number)+".tar.lz4")
		file, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		var writeCloser io.WriteCloser

		if crypter != nil {
			writeCloser, err = crypter.Encrypt(file)

			if err != nil {
				panic(err)
			}

			tarBall.writeCloser = &internal.CascadeWriteCloser{
				WriteCloser: lz4.NewWriter(file),
				Underlying:  &internal.CascadeWriteCloser{WriteCloser: writeCloser, Underlying: file},
			}
		} else {
			writeCloser = file
			tarBall.writeCloser = &internal.CascadeWriteCloser{
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

func (tarBall *FileTarBall) Size() int64            { return tarBall.size }
func (tarBall *FileTarBall) AddSize(i int64)        { tarBall.size += i }
func (tarBall *FileTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }
func (tarBall *FileTarBall) AwaitUploads()          {}

// BufferTarBall represents a tarball that is
// written to buffer.
type BufferTarBall struct {
	name       string
	number     int
	size       int64
	underlying *bytes.Buffer
	tarWriter  *tar.Writer
}

func (tarBall *BufferTarBall) Name() string { return tarBall.name }

func (tarBall *BufferTarBall) SetUp(crypter crypto.Crypter, args ...string) {
	tarBall.tarWriter = tar.NewWriter(tarBall.underlying)
}

func (tarBall *BufferTarBall) CloseTar() error {
	return tarBall.tarWriter.Close()
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
