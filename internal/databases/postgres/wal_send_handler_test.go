package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const walSendTestSegmentSize = 16 * 1024 * 1024

// makeWalSendSegmentData builds a deterministic WAL segment payload
func makeWalSendSegmentData(seed byte) []byte {
	data := make([]byte, walSendTestSegmentSize)
	for i := range data {
		data[i] = byte(i%251) + seed
	}
	return data
}

func putPlainObject(t *testing.T, folder storage.Folder, name string, data []byte) {
	t.Helper()
	err := folder.PutObject(t.Context(), name, bytes.NewReader(data))
	require.NoError(t, err)
}

func putLz4Object(t *testing.T, folder storage.Folder, name string, data []byte) {
	t.Helper()
	var compressed bytes.Buffer
	writer := lz4.Compressor{}.NewWriter(&compressed)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	err = folder.PutObject(t.Context(), name+".lz4", &compressed)
	require.NoError(t, err)
}

// startWalSendTestServer starts the wal-send server on a free port
// and returns its address
func startWalSendTestServer(t *testing.T, folder storage.Folder, pollInterval time.Duration) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go func() {
		_ = postgres.HandleWALSend(ctx, folder, postgres.WalSendOptions{
			Address:      address,
			PollInterval: pollInterval,
		})
	}()

	// wait until the server is ready
	retryDeadline := time.Now().Add(5 * time.Second)
	for {
		probe, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = probe.Close()
			return address
		}
		require.True(t, time.Now().Before(retryDeadline), "wal-send server did not start")
	}
}

func dialWalSendTestServer(t *testing.T, ctx context.Context, address string) *pgconn.PgConn {
	t.Helper()
	connConfig, err := pgconn.ParseConfig("host=127.0.0.1 port=1 sslmode=disable")
	require.NoError(t, err)
	connConfig.RuntimeParams["replication"] = "database"
	connConfig.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}

	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	require.NoError(t, err)
	return conn
}

func TestWalSendServesReplicationProtocol(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	walFolder := folder.GetSubFolder(utility.WalPath)

	segment1 := makeWalSendSegmentData(0)
	segment2 := makeWalSendSegmentData(1)
	putPlainObject(t, walFolder, "000000010000000000000001", segment1)
	putLz4Object(t, walFolder, "000000010000000000000002", segment2)

	historyContent := "1\t0/4000000\tno recovery target specified\n"
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	// IDENTIFY_SYSTEM must report the latest timeline and the end of the archived WAL
	sysInfo, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)
	assert.EqualValues(t, 1, sysInfo.Timeline)
	assert.EqualValues(t, 3*walSendTestSegmentSize, uint64(sysInfo.XLogPos))

	putPlainObject(t, walFolder, "00000002.history", []byte(historyContent))
	// TIMELINE_HISTORY must serve the .history file as-is, like a real primary does
	history, err := pglogrepl.TimelineHistory(ctx, conn, 2)
	require.NoError(t, err)
	assert.Equal(t, "00000002.history", history.FileName)
	assert.Equal(t, historyContent, string(history.Content))

	// START_REPLICATION at the start of the second segment: it streams segment 2,
	// then waits for segment 3 to appear in the storage and streams it too
	startLSN := pglogrepl.LSN(2 * walSendTestSegmentSize)
	err = pglogrepl.StartReplication(ctx, conn, "test_slot", startLSN,
		pglogrepl.StartReplicationOptions{Timeline: 1, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	// upload the third segment after the replication has started,
	// the server must notice it and keep streaming
	segment3 := makeWalSendSegmentData(2)
	putLz4Object(t, walFolder, "000000010000000000000003", segment3)

	received := make([]byte, 0, 2*walSendTestSegmentSize)
	deadline := time.Now().Add(20 * time.Second)
	for uint64(len(received)) < 2*walSendTestSegmentSize {
		require.True(t, time.Now().Before(deadline), "timed out waiting for WAL data")

		msgCtx, msgCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := conn.ReceiveMessage(msgCtx)
		msgCancel()
		require.NoError(t, err)

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok {
			// keepalive or similar, ignore
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.XLogDataByteID:
			xLogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			require.NoError(t, err)
			received = append(received, xLogData.WALData...)

			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: xLogData.WALStart + pglogrepl.LSN(len(xLogData.WALData)),
			})
			require.NoError(t, err)
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			require.NoError(t, err)
			if keepalive.ReplyRequested {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: startLSN + pglogrepl.LSN(len(received)),
				})
				require.NoError(t, err)
			}
		default:
			t.Fatalf("unexpected copy data message type %q", copyData.Data[0])
		}
	}

	expected := make([]byte, 0, len(segment2)+len(segment3))
	expected = append(expected, segment2...)
	expected = append(expected, segment3...)
	if !assert.Equal(t, len(expected), len(received)) {
		// find the first mismatch for debugging
		for i := 0; i < min(len(expected), len(received)); i++ {
			if expected[i] != received[i] {
				t.Fatalf("first mismatch at offset %d: expected %x, received %x",
					i, expected[i:i+8], received[i:i+8])
				break
			}
		}
		return
	}
	assert.True(t, bytes.Equal(expected, received),
		"the streamed WAL must contain the rest of segment 2 followed by segment 3")
}

// TestWalSendIdentifySystemFromBackupSentinel checks that IDENTIFY_SYSTEM values
// are taken from the latest backup sentinel, without listing the WAL folder
func TestWalSendIdentifySystemFromBackupSentinel(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())

	// the WAL folder is empty: the fallback would report timeline 1 and
	// xlogpos 0/0, so any other values must come from the backup sentinel
	backupName := "base_000000070000000000000001"
	sentinel := postgres.BackupSentinelDto{
		SystemIdentifier: &[]uint64{72345678901234}[0],
		PgVersion:        170002,
		BackupStartLSN:   &[]postgres.LSN{postgres.LSN(0xAABB0000)}[0],
		BackupFinishLSN:  &[]postgres.LSN{postgres.LSN(0xAABBCC00)}[0],
	}
	sentinelBody, err := json.Marshal(sentinel)
	require.NoError(t, err)
	err = folder.GetSubFolder(utility.BaseBackupPath).PutObject(t.Context(),
		backupName+utility.SentinelSuffix, bytes.NewReader(sentinelBody))
	require.NoError(t, err)

	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	sysInfo, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	assert.Equal(t, "72345678901234", sysInfo.SystemID)
	assert.EqualValues(t, 7, sysInfo.Timeline)
	assert.EqualValues(t, 0xAABBCC00, uint64(sysInfo.XLogPos))
}

// TestWalSendWaitsForMissingSegment checks that a missing WAL segment does not
// fail the stream: the server enters copy-both mode and keeps retrying the
// direct segment download, sending keepalives in the meantime
func TestWalSendWaitsForMissingSegment(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())

	// the WAL folder is empty: the requested segment is missing
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	startLSN := pglogrepl.LSN(1 * walSendTestSegmentSize)
	err := pglogrepl.StartReplication(ctx, conn, "test_slot", startLSN,
		pglogrepl.StartReplicationOptions{Timeline: 1, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	// the server must send keepalives and no error response while waiting
	gotKeepalive := false
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		msgCtx, msgCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		msg, err := conn.ReceiveMessage(msgCtx)
		msgCancel()
		if err != nil {
			continue // read timeouts are fine here, we just poll for messages
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("unexpected error response while waiting for WAL: %s", m.Message)
		case *pgproto3.CopyData:
			if m.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
				gotKeepalive = true
			}
		}
	}
	assert.True(t, gotKeepalive, "the server must send keepalives while waiting for WAL")
}

// TestWalSendCopyDoneCompletesQuery checks the graceful COPY-both termination:
// after the client sends CopyDone, the server must answer with CopyDone,
// CommandComplete and ReadyForQuery, and the session must continue
func TestWalSendCopyDoneCompletesQuery(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	startLSN := pglogrepl.LSN(1 * walSendTestSegmentSize)
	err := pglogrepl.StartReplication(ctx, conn, "test_slot", startLSN,
		pglogrepl.StartReplicationOptions{Timeline: 1, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	// the client ends the copy
	conn.Frontend().Send(&pgproto3.CopyDone{})
	require.NoError(t, conn.Frontend().Flush())

	gotCopyDone, gotCommandComplete, gotReadyForQuery := false, false, false
	deadline := time.Now().Add(5 * time.Second)
	for !(gotCopyDone && gotCommandComplete && gotReadyForQuery) {
		require.True(t, time.Now().Before(deadline), "timed out waiting for the COPY completion")

		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			// a keepalive, fine
		case *pgproto3.CopyDone:
			gotCopyDone = true
		case *pgproto3.CommandComplete:
			require.Contains(t, []string{"START_STREAMING", "START_REPLICATION"}, string(m.CommandTag))
			gotCommandComplete = true
		case *pgproto3.ReadyForQuery:
			gotReadyForQuery = true
		case *pgproto3.ErrorResponse:
			t.Fatalf("unexpected error response to CopyDone: %s", m.Message)
		default:
			t.Fatalf("unexpected message %T during the COPY completion", msg)
		}
	}

	// the session must continue after the copy: a subsequent query works
	sysInfo, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)
	assert.EqualValues(t, 1, sysInfo.Timeline)
}

// TestWalSendTruncatedSegmentFails checks that a truncated WAL segment is
// reported as an ErrorResponse and closed, never as a successful completion
func TestWalSendTruncatedSegmentFails(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	walFolder := folder.GetSubFolder(utility.WalPath)

	// a truncated segment: far shorter than a real segment
	putPlainObject(t, walFolder, "000000010000000000000001", make([]byte, 100))

	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	startLSN := pglogrepl.LSN(1 * walSendTestSegmentSize)
	err := pglogrepl.StartReplication(ctx, conn, "test_slot", startLSN,
		pglogrepl.StartReplicationOptions{Timeline: 1, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	// the storage failure must surface as an ErrorResponse
	gotError := false
	deadline := time.Now().Add(5 * time.Second)
	for !gotError {
		require.True(t, time.Now().Before(deadline), "timed out waiting for the failure")

		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			// a keepalive, fine
		case *pgproto3.ErrorResponse:
			require.Equal(t, "XX000", m.Code)
			require.Contains(t, m.Message, "000000010000000000000001")
			gotError = true
		case *pgproto3.CommandComplete:
			t.Fatalf("a truncated segment was reported as a success: %q", m.CommandTag)
		default:
			t.Fatalf("unexpected message %T during the failure", msg)
		}
	}

	// the connection is closed by the server after the fatal stream
	// error: the leftover buffered messages end with a read error
	deadline = time.Now().Add(5 * time.Second)
	for {
		_, err = conn.ReceiveMessage(ctx)
		if err != nil {
			break // the server closed the connection
		}
		require.True(t, time.Now().Before(deadline),
			"the connection must be closed after a fatal stream error")
	}
}

// TestWalSendQueryErrorCompletesQuery checks that a failed query completes
// with ReadyForQuery, so that the next command works over the same session
func TestWalSendQueryErrorCompletesQuery(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	// there is no history file in the storage: the query must fail
	_, err := pglogrepl.TimelineHistory(ctx, conn, 2)
	require.Error(t, err)

	// but the query must have been finished with ReadyForQuery,
	// so a subsequent command still works
	sysInfo, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)
	assert.EqualValues(t, 1, sysInfo.Timeline)
}

// walSendTestReceive receives WAL data until the given number of bytes is
// streamed, ignoring keepalives and answering keepalive requests.
// Returns the received WAL payload.
func walSendTestReceive(t *testing.T, ctx context.Context, conn *pgconn.PgConn, wantBytes int) []byte {
	t.Helper()
	received := make([]byte, 0, wantBytes)
	deadline := time.Now().Add(20 * time.Second)
	for len(received) < wantBytes {
		require.True(t, time.Now().Before(deadline), "timed out waiting for WAL data")

		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok {
			continue // keepalive or similar, ignore
		}

		switch copyData.Data[0] {
		case pglogrepl.XLogDataByteID:
			xLogData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			require.NoError(t, err)
			received = append(received, xLogData.WALData...)
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			require.NoError(t, err)
			if keepalive.ReplyRequested {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: keepalive.ServerWALEnd,
				})
				require.NoError(t, err)
			}
		default:
			t.Fatalf("unexpected copy data message type %q", copyData.Data[0])
		}
	}
	return received
}

// walSendTestFinishCopy ends COPY from the client side.
func walSendTestFinishCopy(t *testing.T, ctx context.Context, conn *pgconn.PgConn) {
	t.Helper()
	conn.Frontend().Send(&pgproto3.CopyDone{})
	require.NoError(t, conn.Frontend().Flush())
	walSendTestCopyCompletion(t, ctx, conn, 0, "")
}

// A server-initiated end must wait for the client's CopyDone, report the exact
// next timeline/LSN, and emit no more CopyData after its own CopyDone.
func walSendTestCopyCompletion(t *testing.T, ctx context.Context, conn *pgconn.PgConn, next uint32, switchLSN string) {
	t.Helper()
	gotCopyDone, gotRow := false, false
	var tags []string
	for {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			require.False(t, gotCopyDone, "CopyData after CopyDone")
			require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), m.Data[0])
		case *pgproto3.CopyDone:
			require.False(t, gotCopyDone)
			gotCopyDone = true
			if next != 0 {
				require.NoError(t, pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{ReplyRequested: true}))
				conn.Frontend().Send(&pgproto3.CopyDone{})
				require.NoError(t, conn.Frontend().Flush())
			}
		case *pgproto3.RowDescription:
			require.True(t, gotCopyDone)
			require.Len(t, m.Fields, 2)
			require.Equal(t, "next_tli", string(m.Fields[0].Name))
			require.Equal(t, "next_tli_startpos", string(m.Fields[1].Name))
		case *pgproto3.DataRow:
			require.NotZero(t, next)
			require.Equal(t, [][]byte{[]byte(fmt.Sprint(next)), []byte(switchLSN)}, m.Values)
			gotRow = true
		case *pgproto3.CommandComplete:
			tags = append(tags, string(m.CommandTag))
		case *pgproto3.ReadyForQuery:
			require.True(t, gotCopyDone)
			require.Equal(t, next != 0, gotRow)
			require.Equal(t, []string{"START_STREAMING", "START_REPLICATION"}, tags)
			return
		default:
			t.Fatalf("unexpected COPY completion message: %#v", msg)
		}
	}
}

func TestWalSendPartialSwitchesToNextTimeline(t *testing.T) {
	for _, source := range []string{"partial", "full", "descendant"} {
		t.Run(source, func(t *testing.T) {
			folder := memory.NewFolder("in_memory/", memory.NewKVS())
			walFolder := folder.GetSubFolder(utility.WalPath)
			segment1 := makeWalSendSegmentData(0)
			segment2 := makeWalSendSegmentData(1)
			putPlainObject(t, walFolder, "000000010000000000000001", segment1)
			if source == "partial" {
				putPlainObject(t, walFolder, "000000010000000000000002.partial", segment2[:walSendTestSegmentSize/2])
			} else if source == "full" {
				putPlainObject(t, walFolder, "000000010000000000000002", segment2)
			}
			putPlainObject(t, walFolder, "000000020000000000000002", segment2)
			putPlainObject(t, walFolder, "00000002.history", []byte("1\t0/2800000\tpromotion\n"))
			address := startWalSendTestServer(t, folder, 50*time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			conn := dialWalSendTestServer(t, ctx, address)
			defer func() { _ = conn.Close(context.Background()) }()
			err := pglogrepl.StartReplication(ctx, conn, "test_slot", pglogrepl.LSN(walSendTestSegmentSize),
				pglogrepl.StartReplicationOptions{Timeline: 1, Mode: pglogrepl.PhysicalReplication})
			require.NoError(t, err)
			received := walSendTestReceive(t, ctx, conn, walSendTestSegmentSize+walSendTestSegmentSize/2)
			expected := append(append([]byte{}, segment1...), segment2[:walSendTestSegmentSize/2]...)
			require.True(t, bytes.Equal(expected, received), "old timeline must end exactly at its fork point")
			walSendTestCopyCompletion(t, ctx, conn, 2, "0/2800000")
			_, err = pglogrepl.TimelineHistory(ctx, conn, 2)
			require.NoError(t, err, "connection must be reusable after timeline negotiation")
			err = pglogrepl.StartReplication(ctx, conn, "test_slot", pglogrepl.LSN(2*walSendTestSegmentSize),
				pglogrepl.StartReplicationOptions{Timeline: 2, Mode: pglogrepl.PhysicalReplication})
			require.NoError(t, err)
			received = walSendTestReceive(t, ctx, conn, walSendTestSegmentSize)
			require.True(t, bytes.Equal(segment2, received))
			walSendTestFinishCopy(t, ctx, conn)
		})
	}
}

func TestWalSendStartsAtTimelineEndWithoutCopy(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	putPlainObject(t, folder.GetSubFolder(utility.WalPath), "00000007.history", []byte("1\t0/1800123\tpromotion\n5\t0/2800000\tpromotion\n"))
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()
	results, err := conn.Exec(ctx, "START_REPLICATION 0/1800123 TIMELINE 1").ReadAll()
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, [][]byte{[]byte("5"), []byte("0/1800123")}, results[0].Rows[0])
}

// TestWalSendPartialWithoutSuccessorWaits checks that a segment present in
// the storage only as .partial does not fail the stream when no successor
// timeline is archived yet: the server keeps waiting for the next timeline
// to show up, sending keepalives, like it does for missing segments
func TestWalSendPartialWithoutSuccessorWaits(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	walFolder := folder.GetSubFolder(utility.WalPath)

	// the segment was archived only partially, and no newer timeline exists
	putPlainObject(t, walFolder, "000000010000000000000001.partial", make([]byte, 100))

	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	err := pglogrepl.StartReplication(ctx, conn, "test_slot", pglogrepl.LSN(walSendTestSegmentSize),
		pglogrepl.StartReplicationOptions{Timeline: 1, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	// the server must send keepalives and no error response while waiting
	gotKeepalive := false
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		msgCtx, msgCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		msg, err := conn.ReceiveMessage(msgCtx)
		msgCancel()
		if err != nil {
			continue // read timeouts are fine here, we just poll for messages
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("the stream must wait for the next timeline instead of failing: %s", m.Message)
		case *pgproto3.CopyData:
			if m.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
				gotKeepalive = true
			} else {
				t.Fatalf("unexpected WAL data while no successor timeline exists")
			}
		}
	}
	assert.True(t, gotKeepalive, "the server must send keepalives while waiting for the next timeline")
}

// TestWalSendPartialRejectsForeignTimeline checks that a newer timeline which
// does not descend from the streamed one (a fork of another branch) is not
// streamed: the server keeps waiting. Once the timeline history file proves
// the candidate descends from the streamed timeline, the switch happens
func TestWalSendPartialRejectsForeignTimeline(t *testing.T) {
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	walFolder := folder.GetSubFolder(utility.WalPath)

	// a timeline 5 that forked from timeline 4, not from the streamed timeline 1
	foreignSegment := makeWalSendSegmentData(3)
	putPlainObject(t, walFolder, "000000010000000000000001.partial", foreignSegment[:walSendTestSegmentSize/2])
	putPlainObject(t, walFolder, "000000050000000000000001", foreignSegment)
	putPlainObject(t, walFolder, "00000005.history",
		[]byte("4\t0/1000000\tlost master\n"))

	address := startWalSendTestServer(t, folder, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn := dialWalSendTestServer(t, ctx, address)
	defer func() { _ = conn.Close(context.Background()) }()

	err := pglogrepl.StartReplication(ctx, conn, "test_slot", pglogrepl.LSN(walSendTestSegmentSize),
		pglogrepl.StartReplicationOptions{Timeline: 1, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	// the foreign timeline must not be streamed for a while
	gotKeepalive := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		msgCtx, msgCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		msg, err := conn.ReceiveMessage(msgCtx)
		msgCancel()
		if err != nil {
			continue
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("the stream must wait for the next timeline instead of failing: %s", m.Message)
		case *pgproto3.CopyData:
			if m.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
				gotKeepalive = true
			} else {
				t.Fatalf("timeline 5 (a fork of timeline 4) must not be streamed")
			}
		}
	}
	assert.True(t, gotKeepalive)

	// the history file is replaced by one proving that timeline 5
	// descends from timeline 1: now the switch must happen
	putPlainObject(t, walFolder, "00000005.history",
		[]byte("1\t0/1800000\tpromotion\n"))

	received := walSendTestReceive(t, ctx, conn, walSendTestSegmentSize/2)
	require.True(t, bytes.Equal(foreignSegment[:walSendTestSegmentSize/2], received))
	walSendTestCopyCompletion(t, ctx, conn, 5, "0/1800000")
	err = pglogrepl.StartReplication(ctx, conn, "test_slot", pglogrepl.LSN(walSendTestSegmentSize),
		pglogrepl.StartReplicationOptions{Timeline: 5, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)
	received = walSendTestReceive(t, ctx, conn, walSendTestSegmentSize)
	assert.True(t, bytes.Equal(foreignSegment, received),
		"the streamed WAL must be the whole segment 1 of timeline 5")

	walSendTestFinishCopy(t, ctx, conn)
}
