package utility

import (
	"io"
	"sync"
)

// EmptyWriteIgnorer handles 0 byte write in LZ4 package
// to stop pipe reader/writer from blocking.
type EmptyWriteIgnorer struct {
	io.Writer
}

func (e EmptyWriteIgnorer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return e.Writer.Write(p)
}

type EmptyWriteCloserIgnorer struct {
	io.WriteCloser
}

func (e EmptyWriteCloserIgnorer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return e.WriteCloser.Write(p)
}

func (e EmptyWriteCloserIgnorer) Close() error {
	return e.WriteCloser.Close()
}

// CloseOnce is a wrapper that prevents users from closing io.WriteCloser multiple times
// Note: The behavior of Close after the first call is undefined. (proof: io.Closer comments)
type CloseOnce struct {
	io.WriteCloser
	once sync.Once
	err  error
}

func (c *CloseOnce) Close() error {
	c.once.Do(c.close)
	return c.err
}

func (c *CloseOnce) close() {
	c.err = c.WriteCloser.Close()
}

var _ io.WriteCloser = &CloseOnce{WriteCloser: nil}
