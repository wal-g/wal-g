package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/utility"
)

func TestWalSendStockReceivewalLiveTimelineSwitch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fork    string
		partial bool
	}{
		{"partial", "0/2800000", true},
		{"boundary_without_partial", "0/2000000", false},
		{"prefix_without_partial", "0/2800000", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walSendStockReceivewalLiveTimelineSwitch(t, tc.fork, tc.partial)
		})
	}
}

func walSendStockReceivewalLiveTimelineSwitch(t *testing.T, fork string, partial bool) {
	t.Helper()
	bin := walSendPostgresBin(t)
	folder := memory.NewFolder("archive/", memory.NewKVS())
	version, err := json.Marshal(postgres.BackupSentinelDto{PgVersion: walSendPostgresMajor(t, bin) * 10000})
	require.NoError(t, err)
	putPlainObject(t, folder.GetSubFolder(utility.BaseBackupPath),
		"base_000000010000000000000001"+utility.SentinelSuffix, version)
	walFolder := folder.GetSubFolder(utility.WalPath)
	oldSegment, newSegment := makeWalSendSegmentData(1), makeWalSendSegmentData(2)
	putPlainObject(t, walFolder, "000000010000000000000001", oldSegment)
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000000010000000000000001.partial"), nil, 0600))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(bin, "pg_receivewal"), "-h", "127.0.0.1", "-p", port,
		"-D", dir, "--no-loop", "--no-sync", "--verbose", "--endpos=0/2FFFFFF")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { cancel(); <-done })
	// Receiving the first complete file proves that COPY on timeline 1 is
	// already active before any evidence of the promotion is published.
	require.Eventually(t, func() bool {
		info, err := os.Stat(filepath.Join(dir, "000000010000000000000001"))
		return err == nil && info.Size() == walSendTestSegmentSize
	}, 10*time.Second, 20*time.Millisecond)
	if partial {
		putPlainObject(t, walFolder, "000000010000000000000002.partial", newSegment[:walSendTestSegmentSize/2])
	}
	putPlainObject(t, walFolder, "000000020000000000000002", newSegment)
	historyContent := "1\t" + fork + "\tpromotion\n"
	putPlainObject(t, walFolder, "00000002.history", []byte(historyContent))
	select {
	case err = <-done:
		// Leave the result available for cleanup even on a failed assertion.
		done <- err
		require.NoError(t, err, "pg_receivewal: %s", output.String())
	case <-ctx.Done():
		t.Fatal("pg_receivewal did not follow a promotion on its active stream")
	}
	for name, want := range map[string][]byte{
		"000000010000000000000001": oldSegment,
		"000000020000000000000002": newSegment,
	} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.True(t, bytes.Equal(want, got), "incorrect contents in %s", name)
	}
	history, err := os.ReadFile(filepath.Join(dir, "00000002.history"))
	require.NoError(t, err)
	require.Equal(t, historyContent, string(history))
}
