package tools

import (
	"archive/tar"
	"fmt"
	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g"
	"io"
	"os"
	"path/filepath"
)

// FileTarBall represents a tarball that is
// written to disk.
type FileTarBall struct {
	baseDir string
	trim    string
	out     string
	number  int
	size    int64
	nop     bool
	w       io.WriteCloser
	tw      *tar.Writer
}

// SetUp creates a new LZ4 writer, tar writer and file for
// writing bundled compressed bytes to.
func (fb *FileTarBall) SetUp(names ...string) {
	if fb.tw == nil {
		name := filepath.Join(fb.out, "part_"+fmt.Sprintf("%0.3d", fb.number)+".tar.lz4")
		f, err := os.Create(name)
		if err != nil {
			panic(err)
		}
		fb.w = &walg.Lz4CascadeClose{lz4.NewWriter(f), f}
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

	err = fb.w.Close()
	if err != nil {
		return err
	}
	return nil
}

// Finish alerts that compression is complete.
func (fb *FileTarBall) Finish() error {
	fmt.Printf("Wrote %d compressed tar files to %s.\n", fb.number, fb.out)
	return nil
}

func (fb *FileTarBall) BaseDir() string { return fb.baseDir }
func (fb *FileTarBall) Trim() string    { return fb.trim }
func (fb *FileTarBall) Nop() bool       { return fb.nop }
func (fb *FileTarBall) Number() int     { return fb.number }
func (fb *FileTarBall) Size() int64     { return fb.size }
func (fb *FileTarBall) SetSize(i int64) { fb.size += i }
func (fb *FileTarBall) Tw() *tar.Writer { return fb.tw }

// NOPTarBall mocks a tarball. Used for testing purposes.
type NOPTarBall struct {
	baseDir string
	trim    string
	nop     bool
	number  int
	size    int64
	tw      *tar.Writer
}

func (n *NOPTarBall) SetUp(params ...string) { return }
func (n *NOPTarBall) CloseTar() error        { return nil }
func (n *NOPTarBall) Finish() error {
	fmt.Printf("NOP: %d files.\n", n.number)
	return nil
}

func (n *NOPTarBall) BaseDir() string { return n.baseDir }
func (n *NOPTarBall) Trim() string    { return n.trim }
func (n *NOPTarBall) Nop() bool       { return n.nop }
func (n *NOPTarBall) Number() int     { return n.number }
func (n *NOPTarBall) Size() int64     { return n.size }
func (n *NOPTarBall) SetSize(i int64) { n.size += i }
func (n *NOPTarBall) Tw() *tar.Writer { return n.tw }
