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

func readSnapshots(t *testing.T, dir string) []Snapshot {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "walg-progress-*.json"))
	require.NoError(t, err)
	var snapshots []Snapshot
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Less(t, len(data), maxSnapshotSize)
		var s Snapshot
		require.NoError(t, json.Unmarshal(data, &s))
		snapshots = append(snapshots, s)
	}
	return snapshots
}

func TestProgressFilesSeparateRunsAndReplaceSnapshots(t *testing.T) {
	dir := t.TempDir()
	options := FileOptions{Directory: dir}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			p := StartWithFile(ctx, nil, "backup-push", 80, options)
			ctx = WithTracker(ctx, p)
			for j := 0; j < 20; j++ {
				SetStage(ctx, "packing/uploading")
				_, err := io.Copy(io.Discard, TrackReader(ctx, strings.NewReader("payload")))
				assert.NoError(t, err)
			}
			p.Stop()
		}()
	}
	wg.Wait()
	require.Eventually(t, func() bool { return len(readSnapshots(t, dir)) == 2 }, 3*time.Second, 10*time.Millisecond)
	snapshots := readSnapshots(t, dir)
	assert.NotEqual(t, snapshots[0].RunID, snapshots[1].RunID)
	for _, s := range snapshots {
		assert.Equal(t, "completed", s.Status)
		assert.Equal(t, "packing/uploading", s.Stage)
		assert.Equal(t, int64(140), s.TransferredBytes)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := StartWithFile(ctx, nil, "backup-fetch", 80, FileOptions{Directory: dir})
	defer p.Stop()
	ctx = WithTracker(ctx, p)
	SetStage(ctx, "restoring full base_test")
	BeginDownload(ctx, 4096)
	_, err := io.Copy(io.Discard, TrackReader(ctx, strings.NewReader(strings.Repeat("x", 1024))))
	require.NoError(t, err)
	var first Snapshot
	require.Eventually(t, func() bool {
		ss := readSnapshots(t, dir)
		if len(ss) != 1 || ss[0].DownloadedBytes == nil {
			return false
		}
		first = ss[0]
		return first.TransferredBytes == 1024
	}, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(4096), *first.TotalBytes)
	require.Eventually(t, func() bool {
		s := readSnapshots(t, dir)[0]
		return s.UpdatedAt.After(first.UpdatedAt) && s.StageElapsedSeconds > first.StageElapsedSeconds && s.IdleSeconds > first.IdleSeconds
	}, 3*time.Second, 10*time.Millisecond)
	cancel()
	p.Stop()
	final := readSnapshots(t, dir)[0]
	assert.Equal(t, "canceled", final.Status)
	assert.NotNil(t, final.FinishedAt)
}

func TestFileReporterDoesNotBlockTracker(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
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
	<-entered
	ctx := WithTracker(context.Background(), p)
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
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Error("file write blocked state updates or shutdown")
	}
	assert.Len(t, r.pending, 1, "only the latest snapshot is retained")
	close(release)
	<-r.done
	require.Len(t, writes, 2)
	assert.Equal(t, "completed", writes[1].Status)
	assert.Equal(t, "uploading metadata", writes[1].Stage)
}

func TestFileReporterErrorsAreNonFatalAndReportedOnce(t *testing.T) {
	count := 0
	r := startFileReporter(func(Snapshot) error { return errors.New("disk full") }, func(error) { count++ })
	r.submit(Snapshot{Status: "running"})
	r.submit(Snapshot{Status: "completed"})
	<-r.done
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
	_, err := os.Stat(completed)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(running)
	assert.NoError(t, err)
	_, err = os.Stat(unrelated)
	assert.NoError(t, err)
}

func TestFileOnlyHeartbeatAfterStageChange(t *testing.T) {
	p := newTracker(nil, "backup-fetch", 80)
	p.file = &fileReporter{pending: make(chan Snapshot, 1)}
	ctx := WithTracker(context.Background(), p)
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
	p := StartWithFile(context.Background(), nil, "backup-push", 80, FileOptions{
		Directory: dir, OnError: func(err error) { reported <- err },
	})
	ctx := WithTracker(context.Background(), p)
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
