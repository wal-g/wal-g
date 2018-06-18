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
	baseDir string
	trim    string
	out     string
	number  int
	size    int64
	w       io.WriteCloser
	tw      *tar.Writer
}

// SetUp creates a new LZ4 writer, tar writer and file for
// writing bundled compressed bytes to.
func (fb *FileTarBall) SetUp(crypter walg.Crypter, names ...string) {
	if fb.tw == nil {
		name := filepath.Join(fb.out, "part_"+fmt.Sprintf("%0.3d", fb.number)+".tar.lz4")
		f, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		var wc io.WriteCloser

		if crypter.IsUsed() {
			wc, err = crypter.Encrypt(f)

			if err != nil {
				panic(err)
			}

			fb.w = &walg.Lz4CascadeCloser2{
				Writer:      lz4.NewWriter(f),
				Underlying:  wc,
				Underlying2: f,
			}
		} else {
			wc = f
			fb.w = &walg.Lz4CascadeCloser{
				Writer:     lz4.NewWriter(f),
				Underlying: wc,
			}
		}

		fb.tw = tar.NewWriter(fb.w)
	}
}

// CloseTar closes the tar writer and file, flushing any
// unwritten data to the file before closing.
func (fb *FileTarBall) CloseTar() error {
	err := fb.tw.Close()
	if err != nil {
		return err
	}

	return fb.w.Close()
}

// Finish alerts that compression is complete.
func (fb *FileTarBall) Finish(sentinelDto *walg.S3TarBallSentinelDto) error {
	fmt.Printf("Wrote %d compressed tar files to %s.\n", fb.number, fb.out)
	return nil
}

func (fb *FileTarBall) BaseDir() string { return fb.baseDir }
func (fb *FileTarBall) Trim() string    { return fb.trim }
func (fb *FileTarBall) PartCount() int     { return fb.number }
func (fb *FileTarBall) Size() int64     { return fb.size }
func (fb *FileTarBall) AddSize(i int64) { fb.size += i }
func (fb *FileTarBall) TarWriter() *tar.Writer { return fb.tw }
func (b *FileTarBall) AwaitUploads()    {}

// NOPTarBall mocks a tarball. Used for testing purposes.
type NOPTarBall struct {
	baseDir string
	trim    string
	number  int
	size    int64
	tw      *tar.Writer
}

func (n *NOPTarBall) SetUp(crypter walg.Crypter, params ...string) {}
func (n *NOPTarBall) CloseTar() error                              { return nil }
func (n *NOPTarBall) Finish(sentinelDto *walg.S3TarBallSentinelDto) error {
	fmt.Printf("NOP: %d files.\n", n.number)
	return nil
}

func (n *NOPTarBall) BaseDir() string { return n.baseDir }
func (n *NOPTarBall) Trim() string    { return n.trim }
func (n *NOPTarBall) PartCount() int     { return n.number }
func (n *NOPTarBall) Size() int64     { return n.size }
func (n *NOPTarBall) AddSize(i int64) { n.size += i }
func (n *NOPTarBall) TarWriter() *tar.Writer { return n.tw }
func (b *NOPTarBall) AwaitUploads()   {}
