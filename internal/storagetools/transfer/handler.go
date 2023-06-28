package transfer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type Handler struct {
	source          storage.Folder
	target          storage.Folder
	fileLister      FileLister
	cfg             *HandlerConfig
	fileStatuses    *sync.Map
	filesLeft       int32
	jobRequirements map[jobKey][]jobRequirement
}

type HandlerConfig struct {
	FailOnFirstErr           bool
	Concurrency              int
	AppearanceChecks         uint
	AppearanceChecksInterval time.Duration
}

func NewHandler(
	sourceStorage, targetStorage string,
	fileLister FileLister,
	cfg *HandlerConfig,
) (*Handler, error) {
	sourceFolder, err := multistorage.ConfigureStorageFolder(sourceStorage)
	if err != nil {
		return nil, fmt.Errorf("configure source storage folder: %w", err)
	}
	targetFolder, err := multistorage.ConfigureStorageFolder(targetStorage)
	if err != nil {
		return nil, fmt.Errorf("configure target storage folder: %w", err)
	}

	return &Handler{
		source:          sourceFolder,
		target:          targetFolder,
		fileLister:      fileLister,
		cfg:             cfg,
		fileStatuses:    new(sync.Map),
		jobRequirements: map[jobKey][]jobRequirement{},
	}, nil
}

func (h *Handler) Handle() error {
	files, filesNum, err := h.fileLister.ListFilesToMove(h.source, h.target)
	if err != nil {
		return err
	}

	workersNum := utility.Min(h.cfg.Concurrency, len(files))
	return h.transferConcurrently(workersNum, files, filesNum)
}

type transferJob struct {
	key             jobKey
	prevCheck       time.Time
	performedChecks uint
}

type jobKey struct {
	filePath string
	jobType  jobType
}

type jobType string

const (
	jobTypeCopy   jobType = "copy"
	jobTypeWait   jobType = "wait"
	jobTypeDelete jobType = "delete"
)

type jobRequirement struct {
	filePath  string
	minStatus transferStatus
}

type transferStatus int

const (
	transferStatusFailed transferStatus = iota - 1
	transferStatusNew
	transferStatusCopied
	transferStatusAppeared
	transferStatusDeleted
)

func (ts transferStatus) String() string {
	switch ts {
	case transferStatusNew:
		return "new"
	case transferStatusCopied:
		return "copied"
	case transferStatusAppeared:
		return "appeared"
	case transferStatusDeleted:
		return "deleted"
	case transferStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func (h *Handler) transferConcurrently(workers int, files []FilesGroup, filesNum int) (finErr error) {
	jobsQueue := make(chan transferJob, filesNum)
	h.filesLeft += int32(filesNum)
	for _, group := range files {
		for _, file := range group {
			h.saveRequirements(file)
			h.fileStatuses.Store(file.path, transferStatusNew)
			jobsQueue <- transferJob{
				key: jobKey{
					jobType:  jobTypeCopy,
					filePath: file.path,
				},
			}
		}
	}

	errs := make(chan error, len(files))

	workersCtx, cancelWorkers := context.WithCancel(context.Background())
	cancelOnSignal(cancelWorkers)
	workersWG := new(sync.WaitGroup)
	workersWG.Add(workers)
	for i := 0; i < workers; i++ {
		go h.transferFilesWorker(workersCtx, jobsQueue, errs, workersWG)
	}

	errsWG := new(sync.WaitGroup)
	errsWG.Add(1)
	go func() {
		defer errsWG.Done()
		errsNum := 0
		for e := range errs {
			if h.cfg.FailOnFirstErr {
				cancelWorkers()
				finErr = e
				break
			}
			tracelog.ErrorLogger.PrintError(e)
			errsNum++
			finErr = fmt.Errorf("finished with %d errors", errsNum)
		}
	}()

	workersWG.Wait()
	close(errs)
	errsWG.Wait()

	return finErr
}

func (h *Handler) saveRequirements(file FileToMove) {
	for _, requiredFile := range file.copyAfter {
		job := jobKey{
			filePath: file.path,
			jobType:  jobTypeCopy,
		}
		req := jobRequirement{
			filePath:  requiredFile,
			minStatus: transferStatusAppeared,
		}
		h.jobRequirements[job] = append(h.jobRequirements[job], req)
	}

	for _, requiredFile := range file.deleteAfter {
		job := jobKey{
			filePath: file.path,
			jobType:  jobTypeDelete,
		}
		req := jobRequirement{
			filePath:  requiredFile,
			minStatus: transferStatusDeleted,
		}
		h.jobRequirements[job] = append(h.jobRequirements[job], req)
	}
}

func cancelOnSignal(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		cancel()
	}()
}

func (h *Handler) transferFilesWorker(
	ctx context.Context,
	jobsQueue chan transferJob,
	errs chan error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		var job transferJob

		select {
		case job = <-jobsQueue:
			// Go on
		default:
			// No more files to process, exit
			return
		}

		select {
		case <-ctx.Done():
			// Processing has been canceled, exit
			return
		default:
			// Go on
		}

		var newJob *transferJob
		ok, err := h.checkRequirements(job)
		if ok {
			switch job.key.jobType {
			case jobTypeCopy:
				newJob, err = h.copyFile(job)
			case jobTypeWait:
				newJob, err = h.waitFile(job)
			case jobTypeDelete:
				err = h.deleteFile(job)
			}
		} else {
			// Repeat the same job if its requirements haven't yet satisfied
			newJob = &job
		}

		if err != nil {
			atomic.AddInt32(&h.filesLeft, -1)
			errs <- fmt.Errorf("error with file %q: %s failed: %w", job.key.filePath, job.key.jobType, err)
			continue
		}

		if newJob != nil {
			// Enqueue file again to process it later
			jobsQueue <- *newJob
			continue
		}

		atomic.AddInt32(&h.filesLeft, -1)
		tracelog.InfoLogger.Printf("File is transferred (%d left): %q", atomic.LoadInt32(&h.filesLeft), job.key.filePath)
	}
}

func (h *Handler) checkRequirements(job transferJob) (ok bool, err error) {
	for _, required := range h.jobRequirements[job.key] {
		s, ok := h.fileStatuses.Load(required.filePath)
		if !ok {
			return false, fmt.Errorf("job has a nonexistent requirement")
		}
		actualStatus := s.(transferStatus)
		if actualStatus == transferStatusFailed {
			return false, fmt.Errorf(
				"%s operation requires other file %q to be %s, but it's failed",
				job.key.jobType,
				required.filePath,
				required.minStatus,
			)
		}
		if actualStatus < required.minStatus {
			return false, nil
		}
	}
	return true, nil
}

func (h *Handler) copyFile(job transferJob) (newJob *transferJob, err error) {
	content, err := h.source.ReadObject(job.key.filePath)
	if err != nil {
		return nil, fmt.Errorf("read file from the source storage: %w", err)
	}
	defer utility.LoggedClose(content, "close object content read from the source storage")

	err = h.target.PutObject(job.key.filePath, content)
	if err != nil {
		return nil, fmt.Errorf("write file to the target storage: %w", err)
	}

	h.fileStatuses.Store(job.key.filePath, transferStatusCopied)
	job.key.jobType = jobTypeWait
	newJob = &job

	return newJob, nil
}

func (h *Handler) waitFile(job transferJob) (newJob *transferJob, err error) {
	var appeared bool

	skipCheck := h.cfg.AppearanceChecks == 0
	if skipCheck {
		appeared = true
	} else {
		appeared, err = h.checkForAppearance(job.prevCheck, job.key.filePath)
		if err != nil {
			return nil, err
		}
	}

	if appeared {
		h.fileStatuses.Store(job.key.filePath, transferStatusAppeared)
		job.key.jobType = jobTypeDelete
		newJob = &job
		return newJob, nil
	}

	performedChecks := 1 + job.performedChecks
	if performedChecks >= h.cfg.AppearanceChecks {
		return nil, fmt.Errorf(
			"couldn't wait for the file to appear in the target storage (%d checks performed)",
			h.cfg.AppearanceChecks,
		)
	}

	tracelog.WarningLogger.Printf(
		"Written file hasn't appeared in the target storage (check %d of %d)",
		performedChecks,
		h.cfg.AppearanceChecks,
	)

	job.prevCheck = time.Now()
	job.performedChecks = performedChecks
	newJob = &job

	return newJob, nil
}

func (h *Handler) checkForAppearance(prevCheck time.Time, filePath string) (appeared bool, err error) {
	nextCheck := prevCheck.Add(h.cfg.AppearanceChecksInterval)
	waitTime := time.Until(nextCheck)
	if waitTime > 0 {
		time.Sleep(waitTime)
	}

	appeared, err = h.target.Exists(filePath)
	if err != nil {
		return false, fmt.Errorf("check if file exists in the target storage: %w", err)
	}
	return appeared, nil
}

func (h *Handler) deleteFile(job transferJob) error {
	err := h.source.DeleteObjects([]string{job.key.filePath})
	if err != nil {
		return fmt.Errorf("delete file from the source storage: %w", err)
	}
	h.fileStatuses.Store(job.key.filePath, transferStatusDeleted)
	return nil
}
