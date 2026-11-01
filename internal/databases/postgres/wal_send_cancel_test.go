package postgres

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type walSendObservedConn struct {
	net.Conn
	reading, writingData chan struct{}
}

func (c *walSendObservedConn) Read(p []byte) (int, error) {
	select {
	case c.reading <- struct{}{}:
	default:
	}
	return c.Conn.Read(p)
}

func (c *walSendObservedConn) Write(p []byte) (int, error) {
	if len(p) > 0 && p[0] == 'd' {
		select {
		case c.writingData <- struct{}{}:
		default:
		}
	}
	return c.Conn.Write(p)
}

func walSendAwait(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func walSendPipe(t *testing.T, ctx context.Context, s *walSendServer) (*pgproto3.Frontend, *walSendObservedConn, <-chan struct{}) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	observed := &walSendObservedConn{Conn: server, reading: make(chan struct{}, 1), writingData: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() { defer close(done); s.handleConnection(ctx, observed) }()
	return pgproto3.NewFrontend(client, client), observed, done
}

func walSendPipeStartup(t *testing.T, frontend *pgproto3.Frontend) {
	t.Helper()
	frontend.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"replication": "true", "user": "walg_test"}})
	require.NoError(t, frontend.Flush())
	for {
		msg, err := frontend.Receive()
		require.NoError(t, err)
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return
		}
	}
}

func walSendPipeStartCopy(t *testing.T, frontend *pgproto3.Frontend) {
	t.Helper()
	frontend.SendQuery(&pgproto3.Query{String: "START_REPLICATION PHYSICAL 0/1000000 TIMELINE 1"})
	require.NoError(t, frontend.Flush())
	msg, err := frontend.Receive()
	require.NoError(t, err)
	require.IsType(t, &pgproto3.CopyBothResponse{}, msg)
}

func TestWalSendCancellationUnblocksSocket(t *testing.T) {
	for _, phase := range []string{"startup_read", "query_read", "copy_read", "copy_write"} {
		t.Run(phase, func(t *testing.T) {
			s := walSendRegressionServer(t)
			s.pollInterval = time.Hour // prevent keepalives from masking a blocked WAL write
			if phase == "copy_write" {
				walSendRegressionPut(t, s.walFolder, formatWALFileName(1, 1), make([]byte, WalSegmentSize))
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			frontend, conn, done := walSendPipe(t, ctx, s)
			if phase == "startup_read" {
				walSendAwait(t, conn.reading, "startup read")
			} else {
				walSendPipeStartup(t, frontend)
				if phase == "copy_read" || phase == "copy_write" {
					walSendPipeStartCopy(t, frontend)
				}
				if phase == "copy_write" {
					// net.Pipe has no send buffer: without a receiver, this write
					// stays blocked until cancellation closes the connection.
					walSendAwait(t, conn.writingData, "WAL write")
				}
			}
			cancel()
			walSendAwait(t, done, "connection and streaming goroutines to exit")
		})
	}
}

type walSendCancellableReader struct {
	internal.StorageFolderReader
	reading chan struct{}
	active  atomic.Int32
	closed  atomic.Int32
}

func (r *walSendCancellableReader) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	if name != formatWALFileName(1, 1) {
		return nil, storage.NewObjectNotFoundError(name)
	}
	return &walSendCancellableBody{ctx: ctx, owner: r}, nil
}

type walSendCancellableBody struct {
	ctx   context.Context
	owner *walSendCancellableReader
}

func (b *walSendCancellableBody) Read([]byte) (int, error) {
	b.owner.active.Add(1)
	defer b.owner.active.Add(-1)
	b.owner.reading <- struct{}{}
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *walSendCancellableBody) Close() error { b.owner.closed.Add(1); return nil }

func TestWalSendCancellationCleansBlockedDownloads(t *testing.T) {
	dir := t.TempDir()
	// os.TempDir uses these variables on Unix and Windows respectively.
	for _, key := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(key, dir)
	}
	s := walSendRegressionServer(t)
	s.pollInterval = time.Hour
	r := &walSendCancellableReader{reading: make(chan struct{}, 2)}
	s.reader = r
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	frontend, _, done := walSendPipe(t, ctx, s)
	walSendPipeStartup(t, frontend)
	walSendPipeStartCopy(t, frontend)
	walSendAwait(t, r.reading, "first storage read")
	walSendAwait(t, r.reading, "foreground and prefetch storage reads")
	require.EqualValues(t, 2, r.active.Load())
	paths, err := filepath.Glob(filepath.Join(dir, "walg-wal-send-prefetch-*"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	cancel()
	walSendAwait(t, done, "foreground download and connection to exit")
	require.Eventually(t, func() bool {
		_, err := os.Stat(paths[0])
		return os.IsNotExist(err) && r.active.Load() == 0 && r.closed.Load() >= 2
	}, 5*time.Second, 10*time.Millisecond, "prefetch must close its source and remove its directory")
}
