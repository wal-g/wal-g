package progress

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	maxSnapshotSize   = 16 * 1024
	fileShutdownWait  = 100 * time.Millisecond
	cleanupEntryLimit = 1024
	filePrefix        = "walg-progress-"
)

// FileOptions enables opt-in, best-effort file reporting. Retention only removes
// completed/failed/canceled snapshots; nonpositive retention disables cleanup.
type FileOptions struct {
	Directory string
	Retention time.Duration
	OnError   func(error)
}

type fileReporter struct {
	pending chan Snapshot
	done    chan struct{}
}

func startFileReporter(write func(Snapshot) error, onError func(error)) *fileReporter {
	r := &fileReporter{pending: make(chan Snapshot, 1), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		reported := false
		for s := range r.pending {
			if err := write(s); err != nil && !reported {
				reported = true
				if onError != nil {
					onError(err)
				}
			}
			if s.Status != "running" {
				return
			}
		}
	}()
	return r
}

// submit is called under Tracker.mu. Replace an obsolete pending snapshot;
// neither a slow writer nor a slow error logger can block the tracker.
func (r *fileReporter) submit(s Snapshot) {
	select {
	case r.pending <- s:
		return
	default:
	}
	select {
	case <-r.pending:
	default:
	}
	select {
	case r.pending <- s:
	default:
	}
}

func (r *fileReporter) wait() {
	timer := time.NewTimer(fileShutdownWait)
	defer timer.Stop()
	select {
	case <-r.done:
	case <-timer.C:
	}
}

type stateFile struct {
	options              FileOptions
	runID, path          string
	initialized, cleaned bool
}

func newStateFile(options FileOptions) *stateFile {
	id := uuid.NewString()
	return &stateFile{options: options, runID: id, path: filepath.Join(options.Directory, filePrefix+id+".json")}
}

// write runs only on the file worker. All filesystem work, including startup and
// cleanup, stays off the backup path. UpdatedAt is the sampling time, so a delayed
// write cannot make an old snapshot appear fresh.
func (f *stateFile) write(s Snapshot) error {
	if !f.initialized {
		if err := os.MkdirAll(f.options.Directory, 0700); err != nil {
			return err
		}
		f.initialized = true
	}
	s.Version, s.RunID, s.PID = 1, f.runID, os.Getpid()
	s.Operation, s.Stage = fitText(s.Operation, 128), fitText(s.Stage, 1024)
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(data) >= maxSnapshotSize {
		return fmt.Errorf("progress snapshot exceeds %d bytes", maxSnapshotSize)
	}
	if err := replaceSnapshot(f.path, data); err != nil {
		return err
	}
	if !f.cleaned {
		f.cleaned = true
		if f.options.Retention > 0 {
			return f.cleanup(time.Now().Add(-f.options.Retention))
		}
	}
	return nil
}

func replaceSnapshot(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// cleanup inspects at most 1024 entries in the explicitly configured directory,
// once per run, in bounded batches. Never remove running, malformed, foreign or
// temporary files: an old heartbeat does not prove that its writer has stopped.
func (f *stateFile) cleanup(before time.Time) error {
	dir, err := os.Open(f.options.Directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	for scanned := 0; scanned < cleanupEntryLimit; {
		entries, readErr := dir.ReadDir(min(64, cleanupEntryLimit-scanned))
		scanned += len(entries)
		for _, entry := range entries {
			if !entry.Type().IsRegular() {
				continue
			}
			name := entry.Name()
			if !strings.HasPrefix(name, filePrefix) || !strings.HasSuffix(name, ".json") {
				continue
			}
			id := strings.TrimSuffix(strings.TrimPrefix(name, filePrefix), ".json")
			if parsed, err := uuid.Parse(id); err != nil || parsed.String() != id {
				continue
			}
			path := filepath.Join(f.options.Directory, name)
			if path == f.path {
				continue
			}
			if err := removeExpiredSnapshot(path, id, before); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func removeExpiredSnapshot(path, runID string, before time.Time) error {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxSnapshotSize))
	_ = file.Close()
	if err != nil {
		return err
	}
	var s Snapshot
	if len(data) >= maxSnapshotSize || json.Unmarshal(data, &s) != nil {
		return nil
	}
	if s.Version != 1 || s.RunID != runID || (s.Status != "completed" && s.Status != "canceled" && s.Status != "failed") ||
		s.FinishedAt == nil || s.FinishedAt.IsZero() || !s.FinishedAt.Before(before) {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
