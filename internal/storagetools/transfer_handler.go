package storagetools

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type TransferHandler struct {
	source storage.Folder
	target storage.Folder
	cfg    *TransferHandlerConfig
}

type TransferHandlerConfig struct {
	Prefix                   string
	Overwrite                bool
	FailOnFirstErr           bool
	Concurrency              int
	MaxFiles                 int
	AppearanceChecks         uint
	AppearanceChecksInterval time.Duration
}

func NewTransferHandler(sourceStorage, targetStorage string, cfg *TransferHandlerConfig) (*TransferHandler, error) {
	sourceFolder, err := multistorage.ConfigureStorageFolder(sourceStorage)
	if err != nil {
		return nil, fmt.Errorf("can't configure source storage folder: %w", err)
	}
	targetFolder, err := multistorage.ConfigureStorageFolder(targetStorage)
	if err != nil {
		return nil, fmt.Errorf("can't configure target storage folder: %w", err)
	}

	return &TransferHandler{
		source: sourceFolder,
		target: targetFolder,
		cfg:    cfg,
	}, nil
}

func (h *TransferHandler) Handle() error {
	files, err := h.listFilesToMove()
	if err != nil {
		return err
	}

	workersNum := utility.Min(h.cfg.Concurrency, len(files))
	return h.transferConcurrently(workersNum, files)
}

func (h *TransferHandler) listFilesToMove() ([]storage.Object, error) {
	targetFiles, err := storage.ListFolderRecursivelyWithPrefix(h.target, h.cfg.Prefix)
	if err != nil {
		return nil, fmt.Errorf("can't list files in the target storage: %w", err)
	}
	sourceFiles, err := storage.ListFolderRecursivelyWithPrefix(h.source, h.cfg.Prefix)
	if err != nil {
		return nil, fmt.Errorf("can't list files in the source storage: %w", err)
	}
	tracelog.InfoLogger.Printf("Total files in the source storage: %d", len(sourceFiles))

	missingFiles := make(map[string]storage.Object, len(sourceFiles))
	for _, sourceFile := range sourceFiles {
		missingFiles[sourceFile.GetName()] = sourceFile
	}
	for _, targetFile := range targetFiles {
		if h.cfg.Overwrite {
			sourceFile := missingFiles[targetFile.GetName()]
			logSizesDifference(sourceFile, targetFile)
		} else {
			delete(missingFiles, targetFile.GetName())
		}
	}
	tracelog.InfoLogger.Printf("Files missing in the target storage: %d", len(missingFiles))

	count := 0
	limitedFiles := make([]storage.Object, 0, utility.Min(h.cfg.MaxFiles, len(missingFiles)))
	for _, file := range missingFiles {
		if count >= h.cfg.MaxFiles {
			break
		}
		limitedFiles = append(limitedFiles, file)
		count++
	}
	tracelog.InfoLogger.Printf("Files will be transferred: %d", len(limitedFiles))

	return limitedFiles, nil
}

func logSizesDifference(sourceFile, targetFile storage.Object) {
	if sourceFile.GetSize() != targetFile.GetSize() {
		tracelog.WarningLogger.Printf(
			"File present in both storages and its size is different: %q (source %d bytes VS target %d bytes)",
			targetFile.GetName(),
			sourceFile.GetSize(),
			targetFile.GetSize(),
		)
	}
}

type transferJob struct {
	jobType         transferJobType
	filePath        string
	prevCheck       time.Time
	performedChecks uint
}

type transferJobType int

const (
	transferJobTypeCopy transferJobType = iota
	transferJobTypeDelete
)

func (h *TransferHandler) transferConcurrently(workers int, files []storage.Object) (finErr error) {
	jobsQueue := make(chan transferJob, len(files))
	for _, f := range files {
		jobsQueue <- transferJob{
			jobType:  transferJobTypeCopy,
			filePath: f.GetName(),
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

func cancelOnSignal(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		cancel()
	}()
}

func (h *TransferHandler) transferFilesWorker(
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
		var err error

		switch job.jobType {
		case transferJobTypeCopy:
			newJob, err = h.copyFile(job)
		case transferJobTypeDelete:
			newJob, err = h.deleteFile(job)
		}

		if err != nil {
			errs <- fmt.Errorf("error with file %q: %w", job.filePath, err)
			continue
		}

		if newJob != nil {
			// Enqueue file again to process it later
			jobsQueue <- *newJob
			continue
		}

		tracelog.InfoLogger.Printf("File is transferred (%d left): %q", len(jobsQueue), job.filePath)
	}
}

func (h *TransferHandler) copyFile(job transferJob) (newJob *transferJob, err error) {
	content, err := h.source.ReadObject(job.filePath)
	if err != nil {
		return nil, fmt.Errorf("can't read file from the source storage: %w", err)
	}
	defer utility.LoggedClose(content, "can't close object content read from the source storage")

	err = h.target.PutObject(job.filePath, content)
	if err != nil {
		return nil, fmt.Errorf("can't write file to the target storage: %w", err)
	}

	newJob = &transferJob{
		jobType:  transferJobTypeDelete,
		filePath: job.filePath,
	}

	return newJob, nil
}

func (h *TransferHandler) deleteFile(job transferJob) (newJob *transferJob, err error) {
	var appeared bool

	skipCheck := h.cfg.AppearanceChecks == 0
	if skipCheck {
		appeared = true
	} else {
		appeared, err = h.checkForAppearance(job.prevCheck, job.filePath)
		if err != nil {
			return nil, err
		}
	}

	if appeared {
		err = h.source.DeleteObjects([]string{job.filePath})
		if err != nil {
			return nil, fmt.Errorf("can't delete file from the source storage: %w", err)
		}
		return nil, nil
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

	newJob = &transferJob{
		jobType:         transferJobTypeDelete,
		filePath:        job.filePath,
		prevCheck:       time.Now(),
		performedChecks: performedChecks,
	}

	return newJob, nil
}

func (h *TransferHandler) checkForAppearance(prevCheck time.Time, filePath string) (appeared bool, err error) {
	nextCheck := prevCheck.Add(h.cfg.AppearanceChecksInterval)
	waitTime := time.Until(nextCheck)
	if waitTime > 0 {
		time.Sleep(waitTime)
	}

	appeared, err = h.target.Exists(filePath)
	if err != nil {
		return false, fmt.Errorf("can't check if file exists in the target storage: %w", err)
	}
	return appeared, nil
}
