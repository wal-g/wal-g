package tools

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g"
)

// FileTarBall represents a tarball that is
// written to disk.
type FileTarBall struct {
	archiveDirectory string
	out              string
	number           int
	size             int64
	writeCloser      io.WriteCloser
	tarWriter        *tar.Writer
}

// SetUp creates a new LZ4 writer, tar writer and file for
// writing bundled compressed bytes to.
func (fileTarBall *FileTarBall) SetUp(crypter walg.Crypter, names ...string) {
	if fileTarBall.tarWriter == nil {
		name := filepath.Join(fileTarBall.out, "part_"+fmt.Sprintf("%0.3d", fileTarBall.number)+".tar.lz4")
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

			fileTarBall.writeCloser = &walg.CascadeWriteCloser{
				WriteCloser: lz4.NewWriter(file),
				Underlying: &walg.CascadeWriteCloser{WriteCloser: writeCloser, Underlying: file},
			}
		} else {
			writeCloser = file
			fileTarBall.writeCloser = &walg.CascadeWriteCloser{
				WriteCloser: lz4.NewWriter(file),
				Underlying:  writeCloser,
			}
		}

		fileTarBall.tarWriter = tar.NewWriter(fileTarBall.writeCloser)
	}
}

// CloseTar closes the tar writer and file, flushing any
// unwritten data to the file before closing.
func (fileTarBall *FileTarBall) CloseTar() error {
	err := fileTarBall.tarWriter.Close()
	if err != nil {
		return err
	}

	return fileTarBall.writeCloser.Close()
}

// Finish alerts that compression is complete.
func (fileTarBall *FileTarBall) Finish(sentinelDto *walg.S3TarBallSentinelDto) error {
	fmt.Printf("Wrote %d compressed tar files to %s.\n", fileTarBall.number, fileTarBall.out)
	return nil
}

func (fileTarBall *FileTarBall) ArchiveDirectory() string           { return fileTarBall.archiveDirectory }
func (fileTarBall *FileTarBall) Size() int64            { return fileTarBall.size }
func (fileTarBall *FileTarBall) AddSize(i int64)        { fileTarBall.size += i }
func (fileTarBall *FileTarBall) TarWriter() *tar.Writer { return fileTarBall.tarWriter }
func (fileTarBall *FileTarBall) FileExtension() string { return "lz4" }
func (fileTarBall *FileTarBall) AwaitUploads()          {}

// NOPTarBall mocks a tarball. Used for testing purposes.
type NOPTarBall struct {
	archiveDirectory string
	number           int
	size             int64
	tarWriter        *tar.Writer
}

func (n *NOPTarBall) SetUp(crypter walg.Crypter, params ...string) {}
func (n *NOPTarBall) CloseTar() error                              { return nil }
func (n *NOPTarBall) Finish(sentinelDto *walg.S3TarBallSentinelDto) error {
	fmt.Printf("NOP: %d files.\n", n.number)
	return nil
}

func (n *NOPTarBall) ArchiveDirectory() string    { return n.archiveDirectory }
func (n *NOPTarBall) Size() int64     { return n.size }
func (n *NOPTarBall) AddSize(i int64) { n.size += i }
func (n *NOPTarBall) TarWriter() *tar.Writer { return n.tarWriter }
func (n *NOPTarBall) FileExtension() string { return "lz4" }
func (b *NOPTarBall) AwaitUploads()   {}
