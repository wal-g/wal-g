package postgres

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServe_ReturnsOnContextCancel(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "walg.sock")
	l, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve(ctx, l, nil)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after context cancel")
	}
}
