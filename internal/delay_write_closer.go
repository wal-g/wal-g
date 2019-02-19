package internal

import (
	"io"
)

// DelayWriteCloser delays first writes.
// Encryption starts writing header immediately.
// But there is a lot of places where writer is instantiated long before pipe
// is ready. This is why here is used special writer, which delays encryption
// initialization before actual write. If no write occurs, initialization
// still is performed, to handle zero-byte Files correctly
type DelayWriteCloser struct {
	inner   io.WriteCloser
	crypter Crypter
	outer   *io.WriteCloser
}

func (delayWriteCloser *DelayWriteCloser) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if delayWriteCloser.outer == nil {
		writeCloser, err := delayWriteCloser.crypter.WrapWriter(delayWriteCloser.inner)
		if err != nil {
			return 0, err
		}
		delayWriteCloser.outer = &writeCloser
	}
	n, err = (*delayWriteCloser.outer).Write(p)
	return
}

// Close DelayWriteCloser
func (delayWriteCloser *DelayWriteCloser) Close() error {
	if delayWriteCloser.outer == nil {
		writeCloser, err := delayWriteCloser.crypter.WrapWriter(delayWriteCloser.inner)
		if err != nil {
			return err
		}
		delayWriteCloser.outer = &writeCloser
	}

	return (*delayWriteCloser.outer).Close()
}
