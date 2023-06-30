package buffer

import (
	"bytes"
)

// CloserBuffer defines buffer which wraps bytes.Buffer and has dummy implementation of Closer interface.
type CloserBuffer struct {
	*bytes.Buffer
}

// NewCloserBuffer builds CloserBuffer instance
func NewCloserBuffer() *CloserBuffer {
	return &CloserBuffer{&bytes.Buffer{}}
}

// Close is dummy function that implements Closer interface.
func (cb *CloserBuffer) Close() error {
	return nil
}
