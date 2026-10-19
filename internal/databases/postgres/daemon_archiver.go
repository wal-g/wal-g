package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/statistics"
	"golang.org/x/sync/semaphore"
)

type daemonArchiveJob struct {
	done       chan struct{}
	uploader   *WalUploader
	err        error
	markFailed bool
}

// daemonArchiver owns uploads across archive_command calls. At most C-1 jobs
// are speculative, reserving capacity for the WAL PostgreSQL actually needs.
// The existing ASM remains the persistent success record; only in-flight jobs
// and a bounded lookahead window live in memory.
type daemonArchiver struct {
	ctx                           context.Context //nolint:containedctx // daemon lifetime, not a client connection
	cancel                        context.CancelFunc
	makeUploader                  func(context.Context) (*WalUploader, error)
	manager                       asm.ArchiveStatusManager
	workers, background           *semaphore.Weighted
	window                        int
	timeout                       time.Duration
	preventOverwrite, readyRename bool
	foreground                    chan struct{}
	wake                          chan struct{}
	stopped                       chan struct{}
	wg                            sync.WaitGroup
	mu                            sync.Mutex
	anchor                        string
	jobs                          map[string]*daemonArchiveJob
	retryAfter                    map[string]time.Time
}

func configureDaemonArchiver(ctx context.Context, st *multistorage.Storage) (*daemonArchiver, error) {
	// WAL delta shares recorder state with FlushFiles. Preserve its existing
	// batch lifetime until that mode has its own streaming-scheduler tests.
	if viper.GetBool(conf.UseWalDeltaSetting) {
		tracelog.InfoLogger.Println("WAL delta enabled: retaining batch WAL archiving")
		return nil, nil
	}
	concurrency, err := conf.GetMaxUploadConcurrency()
	if err != nil {
		return nil, err
	}
	timeout, err := conf.GetDurationSetting(conf.PgDaemonWALUploadTimeout)
	if err != nil {
		return nil, err
	}
	window := int(viper.GetInt32(conf.TotalBgUploadedLimit)) - 1
	manager := asm.NewNopASM()
	folder, err := internal.ConfigureArchiveStatusManager()
	if err != nil {
		// Foreground archiving is still useful without the optional success cache.
		tracelog.ErrorLogger.Printf("Disable background WAL archiving: %v", err)
		window = 0
	} else {
		manager = asm.NewDataFolderASM(folder)
	}
	a := newDaemonArchiver(ctx, concurrency, window, timeout, manager,
		func(ctx context.Context) (*WalUploader, error) {
			// Re-evaluate failover for each new WAL, not just at daemon startup.
			return PrepareMultiStorageWalUploader(ctx, st.RootFolder(), "")
		})
	return a, nil
}

func newDaemonArchiver(ctx context.Context, concurrency, window int, timeout time.Duration,
	manager asm.ArchiveStatusManager, makeUploader func(context.Context) (*WalUploader, error)) *daemonArchiver {
	ctx, cancel := context.WithCancel(ctx)
	a := &daemonArchiver{
		ctx: ctx, cancel: cancel, makeUploader: makeUploader, manager: manager,
		workers:    semaphore.NewWeighted(int64(concurrency)),
		background: semaphore.NewWeighted(int64(concurrency - 1)),
		window:     window, timeout: timeout,
		preventOverwrite: viper.GetBool(conf.PreventWalOverwriteSetting),
		readyRename:      viper.GetBool(conf.PgReadyRename),
		foreground:       make(chan struct{}, 1), wake: make(chan struct{}, 1), stopped: make(chan struct{}),
		jobs: make(map[string]*daemonArchiveJob), retryAfter: make(map[string]time.Time),
	}
	if concurrency < 2 {
		a.window = 0
	}
	go a.scan()
	return a
}

func (a *daemonArchiver) Close() {
	a.cancel()
	<-a.stopped
	// Synchronize with the last Add; startJob checks cancellation under this lock.
	a.mu.Lock()
	a.mu.Unlock() //nolint:staticcheck // SA2001: barrier with startJob's final cancellation check and wg.Add
	a.wg.Wait()
}

func (a *daemonArchiver) notify() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *daemonArchiver) Push(ctx context.Context, filename string) error {
	select {
	case a.foreground <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
	defer func() { <-a.foreground }()
	filename = filepath.Clean(filename)
	if isWalFilename(filepath.Base(filename)) {
		a.mu.Lock()
		a.anchor = filename
		a.mu.Unlock()
		a.notify() // Also refill while acknowledging an already uploaded WAL.
	}
	started := time.Now()
	job, err := a.startJob(ctx, filename, false)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.ctx.Done():
		return a.ctx.Err()
	case <-job.done:
	}
	if job.err != nil {
		return job.err
	}
	if err := uploadLocalWalMetadata(ctx, filename, job.uploader.Uploader); err != nil {
		return err
	}
	if isWalFilename(filepath.Base(filename)) {
		if err := a.manager.UnmarkWalFile(filename); err != nil && !os.IsNotExist(err) {
			tracelog.ErrorLogger.Printf("unmark wal-g status for %s: %v", filename, err)
		}
	}
	statistics.WriteS3UploadTimeMetric(time.Since(started))
	return nil // No wait for unrelated speculative uploads.
}

func (a *daemonArchiver) startJob(ctx context.Context, filename string, background bool) (*daemonArchiveJob, error) {
	a.mu.Lock()
	job := a.jobs[filename]
	a.mu.Unlock()
	if job != nil {
		return job, nil
	}
	if background {
		if !a.background.TryAcquire(1) {
			return nil, nil
		}
		if !a.workers.TryAcquire(1) {
			a.background.Release(1)
			return nil, nil
		}
	} else {
		queueCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(a.ctx, cancel)
		err := a.workers.Acquire(queueCtx, 1)
		stop()
		cancel()
		if err != nil {
			return nil, err
		}
	}
	release := func() {
		a.workers.Release(1)
		if background {
			a.background.Release(1)
		}
	}
	a.mu.Lock()
	if err := a.ctx.Err(); err != nil {
		a.mu.Unlock()
		release()
		return nil, err
	}
	if job = a.jobs[filename]; job != nil {
		a.mu.Unlock()
		release()
		return job, nil
	}
	job = &daemonArchiveJob{done: make(chan struct{})}
	a.jobs[filename] = job
	a.wg.Add(1)
	a.mu.Unlock()
	go func() {
		defer a.wg.Done()
		ctx, cancel := context.WithTimeout(a.ctx, a.timeout)
		job.err = a.upload(ctx, filename, job, background)
		cancel()
		if job.err != nil {
			tracelog.ErrorLogger.Printf("WAL upload %s: %v", filename, job.err)
		}
		a.mu.Lock()
		if background && (job.err != nil || job.markFailed) {
			a.retryAfter[filename] = time.Now().Add(time.Second)
		}
		delete(a.jobs, filename)
		close(job.done)
		a.mu.Unlock()
		release()
		a.notify()
	}()
	return job, nil
}

func (a *daemonArchiver) upload(ctx context.Context, filename string, job *daemonArchiveJob, background bool) error {
	uploader, err := a.makeUploader(ctx)
	if err != nil {
		return err
	}
	job.uploader = uploader
	regularWAL := isWalFilename(filepath.Base(filename))
	if regularWAL && a.manager.IsWalAlreadyUploaded(filename) {
		return nil
	}
	if err := uploadWALFile(ctx, uploader, filename,
		a.preventOverwrite || strings.HasSuffix(filename, ".history")); err != nil {
		return err
	}
	// ASM strips extensions: do not create a marker for a .partial file that
	// could later be mistaken for the corresponding complete WAL segment.
	if !regularWAL {
		return nil
	}
	if err := a.manager.MarkWalUploaded(filename); err != nil {
		job.markFailed = true // Avoid a tight re-upload loop if the ASM disk is full.
		tracelog.ErrorLogger.Printf("Error marking WAL %s as uploaded: %v", filename, err)
	} else if background && a.readyRename {
		tracelog.ErrorLogger.PrintOnError(uploader.PGArchiveStatusManager.RenameReady(filepath.Base(filename)))
	}
	return nil
}

func (a *daemonArchiver) scan() {
	defer close(a.stopped)
	if a.window < 1 {
		<-a.ctx.Done()
		return
	}
	ticker := time.NewTicker(pollPauseDuration)
	defer ticker.Stop()
	var scan daemonArchiveScan
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
		case <-a.wake:
		}
		anchor := a.scanAnchor()
		if anchor == "" {
			continue
		}
		candidates := a.candidates(anchor, &scan)
		for _, filename := range candidates {
			if a.ctx.Err() != nil {
				return
			}
			if a.manager.IsWalAlreadyUploaded(filename) {
				continue
			}
			a.mu.Lock()
			wait := time.Now().Before(a.retryAfter[filename])
			a.mu.Unlock()
			if wait {
				continue
			}
			job, err := a.startJob(a.ctx, filename, true)
			if err != nil || job == nil {
				break
			}
		}
	}
}

// Forget retry delays for files outside the current lookahead directory/window.
func (a *daemonArchiver) scanAnchor() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	for filename := range a.retryAfter {
		if filename <= a.anchor || filepath.Dir(filename) != filepath.Dir(a.anchor) {
			delete(a.retryAfter, filename)
		}
	}
	return a.anchor
}

// Prefer arithmetic lookahead and stat to a complete sorted directory listing.
// A throttled fallback handles gaps without repeatedly listing a large backlog.
type daemonArchiveScan struct {
	dir          string
	lastFullScan time.Time
	fallback     []string
}

func (a *daemonArchiver) candidates(anchor string, scan *daemonArchiveScan) []string {
	dir, name := filepath.Dir(anchor), filepath.Base(anchor)
	if scan.dir != dir {
		*scan = daemonArchiveScan{dir: dir}
	}
	files := make([]string, 0, a.window)
	for len(files) < a.window {
		next, err := GetNextWalFilename(name)
		if err != nil {
			break
		}
		if _, err := os.Stat(filepath.Join(dir, archiveStatusDir, next+readySuffix)); err != nil &&
			!a.manager.IsWalAlreadyUploaded(filepath.Join(dir, next)) {
			break
		}
		files = append(files, filepath.Join(dir, next))
		name = next
	}
	if len(files) > 0 {
		return files
	}
	if time.Since(scan.lastFullScan) < 5*time.Second {
		// Reuse the bounded listing on every worker completion. Throttling the
		// listing must not also throttle uploads of files we already found.
		return scan.readyFallback(anchor)
	}
	scan.lastFullScan = time.Now()
	scan.fallback = nil
	entries, err := os.ReadDir(filepath.Join(dir, archiveStatusDir))
	if err != nil {
		tracelog.ErrorLogger.Print(fmt.Errorf("scan archive_status: %w", err))
		return files
	}
	for _, entry := range entries {
		name := strings.TrimSuffix(entry.Name(), readySuffix)
		if !strings.HasSuffix(entry.Name(), readySuffix) || !isWalFilename(name) || name <= filepath.Base(anchor) {
			continue
		}
		files = append(files, filepath.Join(dir, name))
		if len(files) == a.window {
			break
		}
	}
	scan.fallback = files
	return files
}

func (scan *daemonArchiveScan) readyFallback(anchor string) []string {
	var files []string
	for _, filename := range scan.fallback {
		if filename <= anchor {
			continue
		}
		if _, err := os.Stat(filepath.Join(scan.dir, archiveStatusDir, filepath.Base(filename)+readySuffix)); err == nil {
			files = append(files, filename)
		}
	}
	return files
}
