package postgres

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type walSendRecordingConn struct {
	net.Conn
	bytes.Buffer
}

func (c *walSendRecordingConn) Write(p []byte) (int, error) { return c.Buffer.Write(p) }
func (c *walSendRecordingConn) Read(p []byte) (int, error)  { return c.Buffer.Read(p) }
func (*walSendRecordingConn) RemoteAddr() net.Addr          { return &net.TCPAddr{} }

func walSendRegressionClient(s *walSendServer) (*walSendClient, *walSendRecordingConn) {
	conn := &walSendRecordingConn{}
	return &walSendClient{server: s, conn: conn, writer: bufio.NewWriter(conn)}, conn
}

func walSendRegressionServer(t *testing.T) *walSendServer {
	t.Helper()
	root := memory.NewFolder("archive/", memory.NewKVS())
	return &walSendServer{
		rootFolder: root, walFolder: root.GetSubFolder(utility.WalPath),
		reader:       internal.NewFolderReader(root.GetSubFolder(utility.WalPath)),
		pollInterval: time.Millisecond,
	}
}

func walSendRegressionPut(t *testing.T, folder storage.Folder, name string, data []byte) {
	t.Helper()
	require.NoError(t, folder.PutObject(t.Context(), name, bytes.NewReader(data)))
}

func TestWalSendPhysicalClientSetup(t *testing.T) {
	s := walSendRegressionServer(t)
	for _, tc := range []struct{ query, value string }{
		{"SHOW data_directory_mode", "0700"},
		{"SHOW wal_segment_size", fmt.Sprintf("%dMB", WalSegmentSize/(1024*1024))},
	} {
		t.Run(tc.query, func(t *testing.T) {
			c, conn := walSendRegressionClient(s)
			c.handleQuery(t.Context(), []byte(tc.query))
			frontend := pgproto3.NewFrontend(&conn.Buffer, io.Discard)
			msg, err := frontend.Receive()
			require.NoError(t, err)
			require.IsType(t, &pgproto3.RowDescription{}, msg)
			msg, err = frontend.Receive()
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte(tc.value)}, msg.(*pgproto3.DataRow).Values)
		})
	}
	c, conn := walSendRegressionClient(s)
	c.handleQuery(t.Context(), []byte("IDENTIFY_SYSTEM"))
	frontend := pgproto3.NewFrontend(&conn.Buffer, io.Discard)
	_, err := frontend.Receive()
	require.NoError(t, err)
	msg, err := frontend.Receive()
	require.NoError(t, err)
	require.Nil(t, msg.(*pgproto3.DataRow).Values[3], "physical dbname must be SQL NULL")
}

func TestWalSendIdentifyUsesArchiveTimeline(t *testing.T) {
	s := walSendRegressionServer(t)
	sysID, finish := uint64(12345), LSN(WalSegmentSize)
	body, err := json.Marshal(BackupSentinelDto{SystemIdentifier: &sysID, BackupFinishLSN: &finish})
	require.NoError(t, err)
	walSendRegressionPut(t, s.rootFolder.GetSubFolder(utility.BaseBackupPath),
		"base_000000010000000000000000"+utility.SentinelSuffix, body)
	walSendRegressionPut(t, s.walFolder, "00000002.history", []byte("1\t0/1800000\tpromotion\n"))
	walSendRegressionPut(t, s.walFolder, formatWALFileName(2, 3), make([]byte, WalSegmentSize))
	// A longer sibling branch must not determine the selected timeline's end LSN.
	walSendRegressionPut(t, s.walFolder, formatWALFileName(1, 8), make([]byte, WalSegmentSize))
	identify, err := s.identifySystem(t.Context())
	require.NoError(t, err)
	require.Equal(t, "12345", identify.systemIdentifier)
	require.EqualValues(t, 2, identify.timeline)
	require.Equal(t, LSN(4*WalSegmentSize), identify.xlogpos)
}

func TestWalSendHistoryFailsClosed(t *testing.T) {
	for _, history := range []string{"", "1\tnot-an-lsn\tpromotion\n", "1\t0/2000000\tok\ninvalid\n"} {
		t.Run(history, func(t *testing.T) {
			s := walSendRegressionServer(t)
			if history != "" {
				walSendRegressionPut(t, s.walFolder, "00000002.history", []byte(history))
			}
			_, err := s.readTimelineHistory(t.Context(), 2)
			require.Error(t, err)
		})
	}
}

type walSendFailedHistoryReader struct {
	internal.StorageFolderReader
	terminal bool
}

func (r walSendFailedHistoryReader) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.HasPrefix(name, "00000002.history") {
		if r.terminal {
			return io.NopCloser(prefetchErrorReader{bytes.NewReader([]byte("1\t0/1800000\tpromotion\n"))}), nil
		}
		return nil, errors.New("history read unavailable")
	}
	return r.StorageFolderReader.ReadObject(ctx, name)
}

func TestWalSendHistoryReadErrorsDoNotAuthorizeSwitch(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(fmt.Sprint(terminal), func(t *testing.T) {
			s := walSendRegressionServer(t)
			walSendRegressionPut(t, s.walFolder, "00000002.history", []byte("1\t0/1800000\tpromotion\n"))
			s.reader = walSendFailedHistoryReader{StorageFolderReader: s.reader, terminal: terminal}
			c, _ := walSendRegressionClient(s)
			next, err := newWalSendStream(c, startReplicationArgs{}).findSuccessorTimeline(t.Context(), 1, LSN(WalSegmentSize))
			require.Error(t, err)
			require.Nil(t, next)
		})
	}
}

func TestWalSendRejectsForkBehindPosition(t *testing.T) {
	s := walSendRegressionServer(t)
	walSendRegressionPut(t, s.walFolder, "00000002.history", []byte("1\t0/1800000\tpromotion\n"))
	walSendRegressionPut(t, s.walFolder, formatWALFileName(2, 3), make([]byte, WalSegmentSize))
	c, _ := walSendRegressionClient(s)
	st := newWalSendStream(c, startReplicationArgs{})
	st.setPos(LSN(3 * WalSegmentSize))
	next, err := st.findSuccessorTimeline(t.Context(), 1, st.pos())
	require.Error(t, err)
	require.Nil(t, next, "a descendant which forked before the sent WAL cannot continue the stream")
}

func TestWalSendStreamRejectsTerminalError(t *testing.T) {
	data := make([]byte, WalSegmentSize)
	var compressed bytes.Buffer
	w := lz4.NewWriter(&compressed)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	badChecksum := compressed.Bytes()
	badChecksum[len(badChecksum)-1] ^= 1
	for name, reader := range map[string]io.Reader{
		"terminal_error":   prefetchErrorReader{bytes.NewReader(data)},
		"bad_lz4_checksum": lz4.NewReader(bytes.NewReader(badChecksum)),
	} {
		t.Run(name, func(t *testing.T) {
			c, conn := walSendRegressionClient(walSendRegressionServer(t))
			st := newWalSendStream(c, startReplicationArgs{})
			require.Error(t, st.streamSegment(t.Context(), reader, 0, WalSegmentSize))
			frontend := pgproto3.NewFrontend(&conn.Buffer, io.Discard)
			var received int
			for conn.Len() > 0 {
				msg, err := frontend.Receive()
				require.NoError(t, err)
				received += len(msg.(*pgproto3.CopyData).Data) - 25
			}
			require.Less(t, received, int(WalSegmentSize), "failed data must not complete a WAL file")
		})
	}
}

type walSendBlockedPrefetchReader struct {
	started chan struct{}
	release chan struct{}
}

func (r *walSendBlockedPrefetchReader) SubFolder(string) internal.StorageFolderReader { return r }
func (r *walSendBlockedPrefetchReader) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	if name != formatWALFileName(1, 1) {
		return nil, storage.NewObjectNotFoundError(name)
	}
	close(r.started)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.release:
		return io.NopCloser(bytes.NewReader(make([]byte, WalSegmentSize))), nil
	}
}

func TestWalSendPrefetchDiscardsLatePublication(t *testing.T) {
	s := walSendRegressionServer(t)
	c, _ := walSendRegressionClient(s)
	st := newWalSendStream(c, startReplicationArgs{})
	st.setPos(LSN(WalSegmentSize))
	r := &walSendBlockedPrefetchReader{started: make(chan struct{}), release: make(chan struct{})}
	p := &walSendPrefetcher{stream: st, reader: r, timeline: 1, dir: t.TempDir(), window: 1}
	name := formatWALFileName(1, 1)
	done := make(chan error, 1)
	go func() { done <- p.downloadSegment(t.Context(), name) }()
	select {
	case <-r.started:
	case <-time.After(5 * time.Second):
		t.Fatal("prefetch did not start")
	}
	p.remove(name)
	st.setPos(LSN(2 * WalSegmentSize))
	close(r.release)
	require.NoError(t, <-done)
	_, err := os.Stat(filepath.Join(p.dir, name))
	require.True(t, os.IsNotExist(err), "a segment consumed during download must not be published")
}

type walSendCountedFolder struct {
	storage.Folder
	lists atomic.Int32
}

func (f *walSendCountedFolder) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	f.lists.Add(1)
	return f.Folder.ListFolder(ctx)
}

func TestWalSendMissingSegmentDoesNotRescanArchive(t *testing.T) {
	s := walSendRegressionServer(t)
	s.pollInterval = 10 * time.Millisecond
	folder := &walSendCountedFolder{Folder: s.walFolder}
	s.walFolder = folder
	c, _ := walSendRegressionClient(s)
	st := newWalSendStream(c, startReplicationArgs{startLSN: LSN(WalSegmentSize), timeline: 1, explicitTimeli: true})
	require.NoError(t, st.prepare(t.Context()))
	before := folder.lists.Load()
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	_, err := st.run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, before, folder.lists.Load(), "ordinary missing-WAL polls must not list the archive")
}
