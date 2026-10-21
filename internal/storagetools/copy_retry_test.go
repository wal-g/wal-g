package storagetools

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cleanupCrypter struct{ closed atomic.Bool }

func (*cleanupCrypter) Name() string                                              { return "test" }
func (*cleanupCrypter) Decrypt(_ context.Context, r io.Reader) (io.Reader, error) { return r, nil }
func (c *cleanupCrypter) Encrypt(_ context.Context, w io.Writer) (io.WriteCloser, error) {
	return &cleanupWriter{Writer: w, close: func() { c.closed.Store(true) }}, nil
}

type cleanupWriter struct {
	io.Writer
	close func()
}

func (w *cleanupWriter) Close() error { w.close(); return nil }

func TestEncryptCloseWaitsForProducer(t *testing.T) {
	crypter := &cleanupCrypter{}
	r, err := Encrypt(t.Context(), strings.NewReader(strings.Repeat("data", 1000)), crypter)
	require.NoError(t, err)
	_, err = io.CopyN(io.Discard, r, 1)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("encryption producer did not stop after closing its reader")
	}
	require.True(t, crypter.closed.Load(), "encryption writer must close even if the upload fails")
	require.NoError(t, r.Close())
}
