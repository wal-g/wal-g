package postgres

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestWalSend64MBSegmentBoundary(t *testing.T) {
	// WAL size is process-global. Isolate it from other tests and their
	// asynchronously shutting down prefetch goroutines, including under -race.
	if os.Getenv("WALG_TEST_WAL64_CHILD") == "" {
		binary, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestWalSend64MBSegmentBoundary$", "-test.v")
		cmd.Env = append(os.Environ(), "WALG_TEST_WAL64_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	SetWalSize(64)
	s := walSendRegressionServer(t)
	// At 64 MiB, segment 0x3F ends at 1/0. Names based on the default
	// 16 MiB segment count would request the wrong objects at this boundary.
	before, after := "00000001000000000000003F", "000000010000000100000000"
	require.Equal(t, before, formatWALFileName(1, 63))
	require.Equal(t, after, formatWALFileName(1, 64))
	data := bytes.Repeat([]byte{0x73}, int(WalSegmentSize))
	walSendRegressionPut(t, s.walFolder, before, data)
	c, wire := walSendRegressionClient(s)
	st := newWalSendStream(c, startReplicationArgs{
		startLSN: LSN(0xFFFFFF00), timeline: 1, explicitTimeli: true,
	})
	require.NoError(t, st.prepare(t.Context()))
	require.Equal(t, LSN(0xFFFFE000), st.pos(), "resume must align to a WAL page")
	p := &walSendPrefetcher{stream: st, reader: s.reader, timeline: 1, dir: t.TempDir(), window: 2}
	require.NoError(t, p.downloadSegment(t.Context(), before))
	f, ok := p.fetch(before)
	require.True(t, ok)
	info, err := f.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 64*1024*1024, info.Size())
	require.NoError(t, st.streamSegment(t.Context(), f, WalSegmentSize-8192, WalSegmentSize))
	require.NoError(t, f.Close())
	require.Equal(t, LSN(0x100000000), st.pos())
	p.remove(before)
	_, err = os.Stat(filepath.Join(p.dir, before))
	require.True(t, os.IsNotExist(err))
	frontend := pgproto3.NewFrontend(&wire.Buffer, io.Discard)
	msg, err := frontend.Receive()
	require.NoError(t, err)
	payload := msg.(*pgproto3.CopyData).Data
	require.Equal(t, byte('w'), payload[0])
	require.Equal(t, data[len(data)-8192:], payload[25:])
	require.Zero(t, wire.Len())
	st.setPos(LSN(0x100000000))
	walSendRegressionPut(t, s.walFolder, after, data)
	require.NoError(t, p.downloadSegment(t.Context(), after))
	f, ok = p.fetch(after)
	require.True(t, ok, "prefetch must continue with log 1, segment 0")
	require.NoError(t, f.Close())
}
