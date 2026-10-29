package progress

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Accept a collector as well as testing.T so polling failures stay in the check.
func readSnapshots(t require.TestingT, dir string) []Snapshot {
	paths, err := filepath.Glob(filepath.Join(dir, "walg-progress-*.json"))
	require.NoError(t, err)
	var snapshots []Snapshot
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		var s Snapshot
		require.NoError(t, json.Unmarshal(data, &s))
		snapshots = append(snapshots, s)
	}
	return snapshots
}

func awaitSnapshots(t *testing.T, dir string, ready func([]Snapshot) bool) []Snapshot {
	t.Helper()
	var snapshots []Snapshot
	require.EventuallyWithT(t, func(check *assert.CollectT) {
		snapshots = readSnapshots(check, dir)
		require.True(check, ready(snapshots), "snapshot not ready")
	}, 3*time.Second, 10*time.Millisecond)
	return snapshots
}

func waitForSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for progress worker")
	}
}

func TestProgressFilesSeparateRuns(t *testing.T) {
	dir := t.TempDir()
	options := FileOptions{Directory: dir}
	// Both writers are alive together; stream concurrency is tested separately.
	trackers := []*Tracker{
		StartWithFile(t.Context(), nil, "backup-push", 80, options),
		StartWithFile(t.Context(), nil, "backup-push", 80, options),
	}
	for _, p := range trackers {
		t.Cleanup(p.Stop)
		ctx := WithTracker(t.Context(), p)
		SetStage(ctx, "packing/uploading")
		_, err := io.Copy(io.Discard, TrackReader(ctx, strings.NewReader("payload")))
		require.NoError(t, err)
	}
	for _, p := range trackers {
		p.Stop()
	}
	snapshots := awaitSnapshots(t, dir, func(ss []Snapshot) bool {
		return len(ss) == 2 && ss[0].Status == "completed" && ss[1].Status == "completed"
	})
	assert.NotEqual(t, snapshots[0].RunID, snapshots[1].RunID)
	for _, s := range snapshots {
		assert.Equal(t, "packing/uploading", s.Stage)
		assert.Equal(t, int64(7), s.TransferredBytes)
		assert.Nil(t, s.TotalBytes)
		assert.Nil(t, s.ETASeconds)
		assert.NotNil(t, s.FinishedAt)
		assert.NotNil(t, s.LastTransferAt)
		assert.Equal(t, os.Getpid(), s.PID)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 2, "one snapshot per run, no leftover temporary files")
}

func TestProgressFileHeartbeatAndCancellation(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := StartWithFile(ctx, nil, "backup-fetch", 80, FileOptions{Directory: dir})
	defer p.Stop()
	ctx = WithTracker(ctx, p)
	SetStage(ctx, "restoring full base_test")
	BeginDownload(ctx, 4096)
	_, err := io.Copy(io.Discard, TrackReader(ctx, strings.NewReader(strings.Repeat("x", 1024))))
	require.NoError(t, err)
	first := awaitSnapshots(t, dir, func(ss []Snapshot) bool {
		return len(ss) == 1 && ss[0].DownloadedBytes != nil && ss[0].TransferredBytes == 1024
	})[0]
	require.NotNil(t, first.TotalBytes)
	assert.Equal(t, int64(4096), *first.TotalBytes)
	awaitSnapshots(t, dir, func(ss []Snapshot) bool {
		return len(ss) == 1 && ss[0].UpdatedAt.After(first.UpdatedAt) &&
			ss[0].StageElapsedSeconds > first.StageElapsedSeconds && ss[0].IdleSeconds > first.IdleSeconds
	})
	cancel()
	p.Stop()
	final := awaitSnapshots(t, dir, func(ss []Snapshot) bool { return len(ss) == 1 && ss[0].Status == "canceled" })[0]
	assert.NotNil(t, final.FinishedAt)
}

func TestFileReporterDoesNotBlockTracker(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var writes []Snapshot
	r := startFileReporter(func(s Snapshot) error {
		if len(writes) == 0 {
			close(entered)
			<-release
		}
		writes = append(writes, s)
		return nil
	}, nil)
	p := newTracker(nil, "backup-push", 80)
	p.file = r
	p.render(p.started)
	waitForSignal(t, entered)
	ctx := WithTracker(t.Context(), p)
	finished := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			SetStage(ctx, "uploading metadata")
			p.render(p.started.Add(time.Duration(i+1) * time.Second))
		}
		p.finish("completed")
		r.wait()
		close(finished)
	}()
	waitForSignal(t, finished)
	assert.Len(t, r.pending, 1, "only the latest snapshot is retained")
	unblock()
	waitForSignal(t, r.done)
	require.Len(t, writes, 2)
	assert.Equal(t, "completed", writes[1].Status)
	assert.Equal(t, "uploading metadata", writes[1].Stage)
}

func TestFileReporterErrorsAreNonFatalAndReportedOnce(t *testing.T) {
	attempts := make(chan struct{}, 1)
	count := 0
	r := startFileReporter(func(Snapshot) error {
		attempts <- struct{}{}
		return errors.New("disk full")
	}, func(error) { count++ })
	for _, status := range []string{"running", "running", "completed"} {
		r.submit(Snapshot{Status: status})
		waitForSignal(t, attempts) // Prevent coalescing: each write must actually fail.
	}
	waitForSignal(t, r.done)
	assert.Equal(t, 1, count)
}

func TestProgressFileRetentionPreservesRunningAndUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-48 * time.Hour)
	var completed, running string
	for _, status := range []string{"completed", "running"} {
		f := newStateFile(FileOptions{Directory: dir})
		s := Snapshot{Version: 1, Status: status, UpdatedAt: old}
		if status == "completed" {
			s.FinishedAt = &old
			completed = f.path
		} else {
			running = f.path
		}
		require.NoError(t, f.write(s))
	}
	unrelated := filepath.Join(dir, "other.json")
	require.NoError(t, os.WriteFile(unrelated, []byte(`{"status":"completed"}`), 0600))
	f := newStateFile(FileOptions{Directory: dir, Retention: 24 * time.Hour})
	require.NoError(t, f.write(Snapshot{Version: 1, Status: "running", UpdatedAt: time.Now()}))
	assert.NoFileExists(t, completed)
	assert.FileExists(t, running)
	assert.FileExists(t, unrelated)
}

func TestFileOnlyHeartbeatAfterStageChange(t *testing.T) {
	p := newTracker(nil, "backup-fetch", 80)
	p.file = &fileReporter{pending: make(chan Snapshot, 1)}
	ctx := WithTracker(t.Context(), p)
	SetStage(ctx, "reading metadata")
	p.render(p.started.Add(time.Millisecond))
	<-p.file.pending
	// A stage change just after the ticker's start must not suppress its first tick.
	p.render(p.started.Add(time.Second))
	assert.Len(t, p.file.pending, 1)
}

func TestSnapshotReplacementStaysBoundedAndReadable(t *testing.T) {
	f := newStateFile(FileOptions{Directory: t.TempDir()})
	s := Snapshot{Status: "running", Stage: strings.Repeat("x", maxSnapshotSize), UpdatedAt: time.Now()}
	require.NoError(t, f.write(s))
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			data, err := os.ReadFile(f.path)
			if !assert.NoError(t, err) {
				return
			}
			var snapshot Snapshot
			if !assert.NoError(t, json.Unmarshal(data, &snapshot)) {
				return
			}
			assert.Less(t, len(data), maxSnapshotSize)
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	for i := 0; i < 50; i++ {
		s.TransferredBytes++
		assert.NoError(t, f.write(s))
	}
	close(done)
	wg.Wait()
	snapshots := readSnapshots(t, f.options.Directory)
	require.Len(t, snapshots, 1)
	assert.Equal(t, int64(50), snapshots[0].TransferredBytes)
}

func TestUnwritableProgressDirectoryDoesNotFailTransfer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a-file")
	require.NoError(t, os.WriteFile(dir, nil, 0600))
	reported := make(chan error, 1)
	p := StartWithFile(t.Context(), nil, "backup-push", 80, FileOptions{
		Directory: dir, OnError: func(err error) { reported <- err },
	})
	ctx := WithTracker(t.Context(), p)
	n, err := io.Copy(io.Discard, TrackReader(ctx, strings.NewReader("payload")))
	require.NoError(t, err)
	assert.Equal(t, int64(7), n)
	p.Stop()
	select {
	case err := <-reported:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("missing file reporting warning")
	}
}
