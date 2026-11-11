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
	require.LessOrEqual(t, len(batch), 2*a.window)
	listing := scan.listing
	require.NotNil(t, listing)
	scan.pending = batch
	other := filepath.Join(t.TempDir(), filepath.Base(files[0]))
	scan.follow(other)
	require.Empty(t, scan.pending)
	require.Nil(t, scan.listing)
	// Stat can return a native invalid-handle error on Windows. Read reports
	// os.ErrClosed consistently, including for a closed directory handle.
	_, err := listing.Read(make([]byte, 1))
	require.ErrorIs(t, err, os.ErrClosed)
	require.Equal(t, other, scan.cursor)
}

func TestDaemonArchiveScanKeepsSequentialPriorityDuringListing(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 8)
	a := &daemonArchiver{window: 2, manager: asm.NewFakeASM()}
	var scan daemonArchiveScan
	scan.follow(files[0])
	// Give reconciliation a far-ahead entry first, independently of the host
	// filesystem's directory order. The real archive_status also has near WALs.
	listingDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(listingDir, filepath.Base(files[7])+readySuffix), nil, 0600))
	var err error
	scan.listing, err = os.Open(listingDir)
	require.NoError(t, err)
	defer scan.closeListing()

	batch := a.candidates(files[0], &scan)
	require.Equal(t, []string{files[1], files[2], files[7]}, batch,
		"retain listed WALs after the nearer sequential candidates")
	require.Equal(t, files[2], scan.cursor, "far-ahead listed entries must not move the sequential cursor")
}

func TestDaemonArchiverRetriesBeforePendingNewWAL(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 5)
	started := make(chan string, 2)
	a := newDaemonArchiver(t.Context(), 2, 0, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(ctx context.Context, file ioextensions.NamedReader) error {
			started <- file.Name()
			<-ctx.Done()
			return ctx.Err()
		}))
	defer a.Close()
	// A known failure's backoff has expired while newer candidates remain.
	a.mu.Lock()
	a.retryAfter[files[1]] = time.Now().Add(-time.Second)
	a.retryAfter[files[2]] = time.Now().Add(-time.Second)
	a.mu.Unlock()
	scan := daemonArchiveScan{dir: filepath.Dir(files[0]), cursor: files[3], pending: []string{files[4]}}
	a.refill(files[0], &scan)
	select {
	case filename := <-started:
		require.Equal(t, files[1], filename, "old retries must precede new pending WALs")
	case <-time.After(3 * time.Second):
		t.Fatal("no WAL upload started")
	}
	require.Equal(t, []string{files[4]}, scan.pending, "busy workers must retain newer candidates")
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

func TestDaemonArchiverRetainsDueRetryWithoutClientProgress(t *testing.T) {
	files := archiveTestFiles(t, 3)
	a := &daemonArchiver{anchor: files[0], retryAfter: map[string]time.Time{
		files[1]: time.Now().Add(-time.Second),
		files[2]: time.Now().Add(time.Minute),
	}}
	require.Equal(t, files[0], a.scanAnchor())
	require.Contains(t, a.retryAfter, files[1], "expiry must make a retry eligible without losing it")
	require.Contains(t, a.retryAfter, files[2])
	require.Equal(t, files[1], a.retryCandidate(files[0]))
}

// Model successful completion without worker timing. Refill still consumes the
// real pending batch, including whatever cursor maintenance this version owns.
func archiveTestConsumeBatch(t *testing.T, a *daemonArchiver, anchor string,
	scan *daemonArchiveScan, batch []string) {
	t.Helper()
	if len(batch) == 0 {
		return
	}
	for _, filename := range batch {
		require.NoError(t, a.manager.MarkWalUploaded(filename))
	}
	scan.pending = batch
	a.refill(anchor, scan)
	require.Empty(t, scan.pending)
}

func TestDaemonArchiveScanContiguousArrivalBeyondGap(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 5)
	status := filepath.Join(filepath.Dir(files[0]), archiveStatusDir)
	require.NoError(t, os.Remove(filepath.Join(status, filepath.Base(files[1])+readySuffix)))
	newReady := filepath.Join(status, filepath.Base(files[4])+readySuffix)
	require.NoError(t, os.Remove(newReady))
	a := &daemonArchiver{ctx: t.Context(), window: 2, manager: asm.NewFakeASM()}
	var scan daemonArchiveScan
	defer scan.closeListing()
	for range 12 {
		batch := a.candidates(files[0], &scan)
		archiveTestConsumeBatch(t, a, files[0], &scan, batch)
		if scan.listing == nil && len(batch) == 0 {
			break
		}
	}
	require.True(t, a.manager.IsWalAlreadyUploaded(files[2]))
	require.True(t, a.manager.IsWalAlreadyUploaded(files[3]))
	require.Nil(t, scan.listing, "the first pass must have reached EOF")
	require.True(t, time.Now().Before(scan.nextRescan))
	require.NoError(t, os.WriteFile(newReady, nil, 0600))
	batch := a.candidates(files[0], &scan)
	t.Logf("after discovering the run beyond the gap: cursor=%s; new candidates=%v",
		filepath.Base(scan.cursor), batch)
	require.Contains(t, batch, files[4],
		"a contiguous arrival after an already discovered run must not need another directory pass")
}

func TestDaemonArchiveScanRetainsMixedBatch(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 8)
	status := filepath.Join(filepath.Dir(files[0]), archiveStatusDir)
	require.NoError(t, os.Remove(filepath.Join(status, filepath.Base(files[4])+readySuffix)))
	a := &daemonArchiver{ctx: t.Context(), window: 2, manager: asm.NewFakeASM()}
	var scan daemonArchiveScan
	scan.follow(files[0])
	scan.cursor = files[3]
	scan.lastFullScan = time.Now()
	// Supply an exact bounded reconciliation chunk, independent of filesystem
	// ordering. Both names still have real .ready files in archive_status.
	listingDir := t.TempDir()
	for _, filename := range []string{files[1], files[7]} {
		require.NoError(t, os.WriteFile(filepath.Join(listingDir, filepath.Base(filename)+readySuffix), nil, 0600))
	}
	var err error
	scan.listing, err = os.Open(listingDir)
	require.NoError(t, err)
	defer scan.closeListing()
	var seen []string
	for range 8 {
		batch := a.candidates(files[0], &scan)
		seen = append(seen, batch...)
		archiveTestConsumeBatch(t, a, files[0], &scan, batch)
		if scan.listing == nil && len(batch) == 0 {
			break
		}
	}
	require.Nil(t, scan.listing)
	require.True(t, time.Now().Before(scan.nextRescan))
	t.Logf("candidates retained from the complete reconciliation chunk: %v", seen)
	require.Contains(t, seen, files[1])
	require.Contains(t, seen, files[7],
		"prioritizing the old hole must retain the known newer WAL for available workers")
}

// A retained far-ahead entry must not displace the next prediction batch.
// Drive refill manually so each completion gives exactly one worker slot.
func TestDaemonArchiverRefillsBeforeFarListedWAL(t *testing.T) {
	archiveTestConfig(t)
	files := archiveTestFiles(t, 8)
	started, release := make(chan string, 1), make(chan struct{})
	a := newDaemonArchiver(t.Context(), 2, 2, time.Minute, asm.NewFakeASM(), archiveTestFactory(
		func(ctx context.Context, file ioextensions.NamedReader) error {
			started <- file.Name()
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}))
	defer a.Close()
	// No Push establishes an anchor, so the scanner cannot add any jobs.
	var scan daemonArchiveScan
	scan.follow(files[0])
	listingDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(listingDir, filepath.Base(files[7])+readySuffix), nil, 0600))
	var err error
	scan.listing, err = os.Open(listingDir)
	require.NoError(t, err)
	defer scan.closeListing()
	a.refill(files[0], &scan)
	require.Contains(t, scan.pending, files[7], "retain the listed WAL while closer work runs")
	for i := 1; i < len(files); i++ {
		if i > 1 {
			a.refill(files[0], &scan)
		}
		select {
		case filename := <-started:
			require.Equal(t, files[i], filename, "prefer the smallest ready WAL across prediction batches")
		case <-time.After(3 * time.Second):
			t.Fatal("no WAL upload started")
		}
		require.LessOrEqual(t, len(scan.pending), 2*a.window)
		release <- struct{}{}
		a.wg.Wait() // The manual scheduler is the only producer of jobs.
	}
}
