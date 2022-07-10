package greenplum

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

type SegBackupRunner struct {
	// content ID of the segment backup
	contentID int
	// name of the main backup
	backupName string
	// args for the backup-push command
	backupArgs string
	// controls the frequency of the backup state updates
	stateUpdateInterval time.Duration
}

func NewSegBackupRunner(contentID int, backupName, backupArgs string, updInterval time.Duration) *SegBackupRunner {
	return &SegBackupRunner{
		contentID:           contentID,
		backupName:          backupName,
		backupArgs:          backupArgs,
		stateUpdateInterval: updInterval,
	}
}

func (r *SegBackupRunner) Run() {
	contentIDArg := fmt.Sprintf("--content-id=%d", r.contentID)
	cmdArgs := []string{"seg", "backup-push", contentIDArg, "--gp-composer"}
	backupArgs := strings.Fields(r.backupArgs)
	cmdArgs = append(cmdArgs, backupArgs...)

	if internal.CfgFile != "" {
		cmdArgs = append(cmdArgs, "--config", internal.CfgFile)
	}

	segBackupStatesPath := FormatSegmentStateFolderPath(r.contentID)
	tracelog.ErrorLogger.FatalOnError(os.RemoveAll(segBackupStatesPath))
	tracelog.ErrorLogger.FatalOnError(os.MkdirAll(segBackupStatesPath, os.ModePerm))

	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = os.Environ()

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	tracelog.InfoLogger.Printf("starting the backup-push command: %v", cmd)

	err := cmd.Start()
	tracelog.ErrorLogger.FatalfOnError("backup-push start failed: %v", err)

	done := make(chan error)
	go func() {
		done <- cmd.Wait()
	}()

	err = r.waitBackup(cmd, done)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (r *SegBackupRunner) waitBackup(cmd *exec.Cmd, doneCh chan error) error {
	ticker := time.NewTicker(r.stateUpdateInterval)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		status, err := checkBackupStatus(ticker, doneCh, sigCh)
		saveErr := writeBackupState(SegBackupState{Status: status, TS: time.Now()}, r.contentID, r.backupName)
		if saveErr != nil {
			tracelog.WarningLogger.Printf("Failed to update the backup status file: %v", saveErr)
			if status != RunningBackupStatus {
				// must exit to avoid endless loop
				return nil
			}
		}

		switch status {
		case SuccessBackupStatus:
			tracelog.InfoLogger.Println("backup-push success")
			return nil
		case FailedBackupStatus:
			return fmt.Errorf("backup-push failed: %v", err)
		case InterruptedBackupStatus:
			// on receiving a SIGTERM, also broadcast it to the backup process
			if termErr := cmd.Process.Signal(syscall.SIGTERM); termErr != nil {
				tracelog.ErrorLogger.Printf("failed to send SIGTERM to the backup process: %v", termErr)
			}
			return fmt.Errorf("backup-push terminated")
		}
	}
}

// TODO: unit tests
func checkBackupStatus(ticker *time.Ticker, doneCh chan error, sigCh chan os.Signal) (SegBackupStatus, error) {
	select {
	case <-ticker.C:
		tracelog.DebugLogger.Printf("Tick")
		return RunningBackupStatus, nil

	case err := <-doneCh:
		if err != nil {
			return FailedBackupStatus, err
		}

		return SuccessBackupStatus, nil

	case sig := <-sigCh:
		tracelog.ErrorLogger.Printf("Received signal: %s, terminating the running backup...", sig)
		return InterruptedBackupStatus, nil
	}
}

func writeBackupState(state SegBackupState, contentID int, backupName string) error {
	statePath := FormatBackupStatePath(contentID, backupName)
	dstFile, err := os.OpenFile(statePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		return fmt.Errorf("failed to open the backup state file: %v", err)
	}
	bytes, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = dstFile.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}
