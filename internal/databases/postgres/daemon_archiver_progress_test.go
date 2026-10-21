package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/ioextensions"
)

// Match the reported backlog: the foreground and one speculative upload stay
// blocked, but another worker must drain more than 5000 ready WALs without a
// second client request. Test both contiguous prediction and sparse listing.
func TestDaemonArchiverDrainsBeyondClientWindow(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		t.Run(fmt.Sprintf("sparse=%v", sparse), func(t *testing.T) {
			archiveTestConfig(t)
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, archiveStatusDir), 0700))
			const count = 5005
			files := make([]string, count)
			name := "000000010000000000000000"
			for i := range files {
				files[i] = filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(files[i], []byte("test WAL"), 0600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, archiveStatusDir, name+readySuffix), nil, 0600))
				var err error
				name, err = GetNextWalFilename(name)
				require.NoError(t, err)
				if sparse {
					name, err = GetNextWalFilename(name)
					require.NoError(t, err)
				}
			}
			var completed, active, peak atomic.Int32
			started := make(chan struct{})
			manager := asm.NewFakeASM()
			a := newDaemonArchiver(t.Context(), 3, 1023, time.Minute, manager, archiveTestFactory(
				func(ctx context.Context, file ioextensions.NamedReader) error {
					n := active.Add(1)
					defer active.Add(-1)
					for old := peak.Load(); n > old; old = peak.Load() {
						if peak.CompareAndSwap(old, n) {
							break
						}
					}
					if file.Name() == files[0] {
						close(started)
					}
					if file.Name() == files[0] || file.Name() == files[1] {
						<-ctx.Done()
						return ctx.Err()
					}
					completed.Add(1)
					return nil
				}))
			defer a.Close()
			foreground := make(chan error, 1)
			go func() { foreground <- a.Push(t.Context(), files[0]) }()
			archiveTestWait(t, started)
			require.Eventually(t, func() bool { return completed.Load() == count-2 }, 20*time.Second, 10*time.Millisecond)
			require.LessOrEqual(t, peak.Load(), int32(3))
			select {
			case err := <-foreground:
				t.Fatalf("acknowledged blocked foreground WAL: %v", err)
			default:
			}
			require.False(t, manager.IsWalAlreadyUploaded(files[0]))
			require.False(t, manager.IsWalAlreadyUploaded(files[1]))
			a.Close()
			require.ErrorIs(t, <-foreground, context.Canceled)
		})
	}
}

func TestDaemonArchiverRevisitsFailuresAndLateReady(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 8)
	lateReady := filepath.Join(filepath.Dir(files[2]), archiveStatusDir, filepath.Base(files[2])+readySuffix)
	require.NoError(t, os.Remove(lateReady))
	manager := asm.NewFakeASM()
	var attempts atomic.Int32
	a := newDaemonArchiver(t.Context(), 3, 2, time.Minute, manager, archiveTestFactory(
		func(_ context.Context, file ioextensions.NamedReader) error {
			if file.Name() == files[1] && attempts.Add(1) == 1 {
				return errors.New("injected storage failure")
			}
			return nil
		}))
	defer a.Close()
	archiveTestPush(t, a, files[0])
	require.Eventually(t, func() bool { return manager.IsWalAlreadyUploaded(files[7]) }, 3*time.Second, time.Millisecond)
	require.False(t, manager.IsWalAlreadyUploaded(files[1]))
	require.False(t, manager.IsWalAlreadyUploaded(files[2]))
	require.NoError(t, os.WriteFile(lateReady, nil, 0600))
	require.Eventually(t, func() bool {
		return manager.IsWalAlreadyUploaded(files[1]) && manager.IsWalAlreadyUploaded(files[2])
	}, 12*time.Second, 10*time.Millisecond, "a later directory pass must revisit files behind the cursor")
	require.GreaterOrEqual(t, attempts.Load(), int32(2))
}

func TestDaemonArchiveScanBoundsAndClosesListing(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 8)
	a := &daemonArchiver{window: 2, manager: asm.NewFakeASM()}
	var scan daemonArchiveScan
	defer scan.closeListing()
	scan.follow(files[0])
	scan.nextRescan = time.Time{} // Force reconciliation, rather than prediction.
	batch := a.candidates(files[0], &scan)
	require.LessOrEqual(t, len(batch), a.window)
	listing := scan.listing
	require.NotNil(t, listing)
	scan.pending = batch
	other := filepath.Join(t.TempDir(), filepath.Base(files[0]))
	scan.follow(other)
	require.Empty(t, scan.pending)
	require.Nil(t, scan.listing)
	_, err := listing.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.Equal(t, other, scan.cursor)
}

func TestDaemonArchiverNewReadyWithoutClientRequest(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 4)
	ready := filepath.Join(filepath.Dir(files[3]), archiveStatusDir, filepath.Base(files[3])+readySuffix)
	require.NoError(t, os.Remove(ready))
	manager := asm.NewFakeASM()
	a := newDaemonArchiver(t.Context(), 2, 1, time.Minute, manager,
		archiveTestFactory(func(context.Context, ioextensions.NamedReader) error { return nil }))
	defer a.Close()
	archiveTestPush(t, a, files[0])
	require.Eventually(t, func() bool { return manager.IsWalAlreadyUploaded(files[2]) }, 3*time.Second, time.Millisecond)
	require.NoError(t, os.WriteFile(ready, nil, 0600))
	require.Eventually(t, func() bool { return manager.IsWalAlreadyUploaded(files[3]) }, 3*time.Second, time.Millisecond,
		"contiguous arrivals must not wait for a new client request or five-second reconciliation")
}

func TestDaemonArchiverExpiresRetryStateWithoutClientProgress(t *testing.T) {
	files := archiveTestFiles(t, 3)
	a := &daemonArchiver{anchor: files[0], retryAfter: map[string]time.Time{
		files[1]: time.Now().Add(-time.Second),
		files[2]: time.Now().Add(time.Minute),
	}}
	require.Equal(t, files[0], a.scanAnchor())
	require.NotContains(t, a.retryAfter, files[1])
	require.Contains(t, a.retryAfter, files[2])
}
