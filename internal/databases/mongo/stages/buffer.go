package stages

import (
	"bytes"
	"fmt"
	"io"
	"os"

	mocks "github.com/wal-g/wal-g/internal/databases/mongo/stages/mocks"
)

var (
	_ = []Buffer{&FileBuffer{}, &MemoryBuffer{}, &mocks.Buffer{}}
)

// Buffer defines interface to accumulate bytes.
type Buffer interface {
	io.Writer
	io.Closer
	Reader() (io.Reader, error)
	Reset() error
	Len() int
}

// FileBuffer implements Buffer interface with filesystem backend.
type FileBuffer struct {
	*os.File
	len     int
	reading bool
}

// NewFileBuffer builds FileBuffer with given args.
func NewFileBuffer(path string) (*FileBuffer, error) {
	bufFile, err := os.CreateTemp(path, "walg.oplog_push.tmp.")
	if err != nil {
		return nil, err
	}
	return &FileBuffer{File: bufFile}, nil
}

// Close closes FileBuffer and removes temporary file.
func (fb *FileBuffer) Close() error {
	if err := fb.File.Close(); err != nil {
		return err
	}
	if err := os.Remove(fb.Name()); err != nil {
		return err
	}
	return nil
}

// Reader provides io.Reader.
func (fb *FileBuffer) Reader() (io.Reader, error) {
	if _, err := fb.Seek(0, 0); err != nil {
		return nil, err
	}
	fb.reading = true
	return fb, nil
}

// Reset truncates buffer.
func (fb *FileBuffer) Reset() error {
	fb.len = 0
	if err := fb.Truncate(0); err != nil {
		return err
	}
	if _, err := fb.Seek(0, 0); err != nil {
		return err
	}
	fb.reading = false
	return nil
}

// Write writes bytes to the buffer.
func (fb *FileBuffer) Write(p []byte) (n int, err error) {
	if fb.reading {
		return 0, fmt.Errorf("buffer is not fully readed or reset")
	}

	n, err = fb.File.Write(p)
	fb.len += n
	return n, err
}

// Read reads the next len(p) bytes from the buffer or until the buffer is drained.
func (fb *FileBuffer) Read(p []byte) (n int, err error) {
	n, err = fb.File.Read(p)
	fb.len -= n
	return n, err
}

// Len returns the number of bytes of the unread portion of the buffer
// we assume small files only, so cast to int to match bytes.Buffer interface
func (fb *FileBuffer) Len() int {
	return fb.len
}

type MemoryBuffer struct {
	*bytes.Buffer
	reading bool
}

// NewMemoryBuffer builds MemoryBuffer.
func NewMemoryBuffer() *MemoryBuffer {
	return &MemoryBuffer{&bytes.Buffer{}, false}
}

// Write writes bytes to the buffer.
func (mb *MemoryBuffer) Write(p []byte) (n int, err error) {
	if mb.reading {
		return 0, fmt.Errorf("buffer is not fully readed or reset")
	}
	return mb.Buffer.Write(p)
}

// Reader provides io.Reader.
func (mb *MemoryBuffer) Reader() (io.Reader, error) {
	mb.reading = true
	return mb, nil
}

// Close closes FileBuffer and removes temporary file.
func (mb *MemoryBuffer) Close() error {
	return mb.Reset()
}

// Reset truncates buffer.
func (mb *MemoryBuffer) Reset() error {
	mb.reading = false
	mb.Buffer.Reset()
	return nil
}
