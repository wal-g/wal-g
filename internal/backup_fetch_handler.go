package internal

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
)

type BackupNonExistenceError struct {
	error
}

type StreamFetcher = func(backup Backup, writeCloser io.WriteCloser) error

func NewBackupNonExistenceError(backupName string) BackupNonExistenceError {
	return BackupNonExistenceError{errors.Errorf("Backup '%s' does not exist.", backupName)}
}

func (err BackupNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// GetBackupToCommandFetcher returns function that copies all bytes from backup to cmd's stdin
func GetBackupToCommandFetcher(cmd *exec.Cmd) func(folder storage.Folder, backup Backup) {
	return func(folder storage.Folder, backup Backup) {
		stdin, err := cmd.StdinPipe()
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
		stderr := &bytes.Buffer{}
		cmd.Stderr = stderr
		err = cmd.Start()
		tracelog.ErrorLogger.FatalfOnError("Failed to start restore command: %v\n", err)

		fetcher, err := GetBackupStreamFetcher(backup)
		tracelog.ErrorLogger.FatalfOnError("Failed to detect backup format: %v\n", err)

		err = fetcher(backup, stdin)

		cmdErr := cmd.Wait()
		if err != nil || cmdErr != nil {
			tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
		}
		if cmdErr != nil {
			if err != nil {
				tracelog.ErrorLogger.Printf("Failed to fetch backup: %v\n", err)
			}
			err = cmdErr
		}
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// StreamBackupToCommandStdin downloads and decompresses backup stream to cmd stdin.
func StreamBackupToCommandStdin(cmd *exec.Cmd, backup Backup) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to fetch backup: %v", err)
	}
	tracelog.DebugLogger.Printf("Running command: %s", cmd.Args)
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start command: %v", err)
	}
	err = DownloadAndDecompressStream(backup, stdin)
	if err != nil {
		return errors.Wrap(err, "failed to download and decompress stream")
	}
	err = cmd.Wait()
	if err != nil {
		return err
	}
	if cmd.ProcessState != nil && !cmd.ProcessState.Success() {
		return fmt.Errorf("command exited with non-zero code: %d", cmd.ProcessState.ExitCode())
	}
	return nil
}

type Fetcher func(folder storage.Folder, backup Backup)

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder, targetBackupSelector BackupSelector, fetcher Fetcher) {
	backupName, storageName, err := targetBackupSelector.Select(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s)\n", backupName)

	multistorage.SetPolicies(folder, policies.TakeFirstStorage)
	err = multistorage.UseSpecificStorage(storageName, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fix the storage where the backup is from: %v\n", err)

	backup, _, err := GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

	fetcher(folder, backup)
}
