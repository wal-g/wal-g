package computils

import "io"

type ReaderFromWriteCloser interface {
	io.ReaderFrom
	io.WriteCloser
}

