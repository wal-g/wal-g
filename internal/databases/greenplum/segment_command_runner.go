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

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/tracelog"
)

type SegCmdRunner struct {
	// content ID of the segment
	contentID int
	// name of the command
	cmdName string
	// args for the command
	cmdArgs string
	// controls the frequency of the command execution state updates
	stateUpdateInterval time.Duration
}

func NewSegCmdRunner(contentID int, cmdName, cmdArgs string, updInterval time.Duration) *SegCmdRunner {
	return &SegCmdRunner{
		contentID:           contentID,
		cmdName:             cmdName,
		cmdArgs:             cmdArgs,
		stateUpdateInterval: updInterval,
	}
}

func (r *SegCmdRunner) Run() {
	args := []string{r.cmdName, fmt.Sprintf("--content-id=%d", r.contentID)}
	args = append(args, strings.Fields(r.cmdArgs)...)

	if internal.CfgFile != "" {
		args = append(args, "--config", internal.CfgFile)
	}

	segCmdStatesPath := FormatSegmentStateFolderPath(r.contentID)
	tracelog.ErrorLogger.FatalOnError(os.RemoveAll(segCmdStatesPath))
	tracelog.ErrorLogger.FatalOnError(os.MkdirAll(segCmdStatesPath, os.ModePerm))

	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = os.Environ()

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	tracelog.InfoLogger.Printf("starting the command: %v", cmd)

	err := cmd.Start()
	tracelog.ErrorLogger.FatalfOnError("command start failed: %v", err)

	done := make(chan error)
	go func() {
		done <- cmd.Wait()
	}()

	err = r.waitCmd(cmd, done)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (r *SegCmdRunner) waitCmd(cmd *exec.Cmd, doneCh chan error) error {
	ticker := time.NewTicker(r.stateUpdateInterval)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		status, err := checkCmdStatus(ticker, doneCh, sigCh)
		saveErr := writeCmdState(SegCmdState{Status: status, TS: time.Now()}, r.contentID, r.cmdName)
		if saveErr != nil {
			tracelog.WarningLogger.Printf("Failed to update the command status file: %v", saveErr)
			if status != RunningCmdStatus {
				// must exit to avoid endless loop
				return nil
			}
		}

		switch status {
		case SuccessCmdStatus:
			tracelog.InfoLogger.Println("command success")
			return nil
		case FailedCmdStatus:
			return fmt.Errorf("command failed: %v", err)
		case InterruptedCmdStatus:
			// on receiving a SIGTERM, also broadcast it to the command process
			if termErr := cmd.Process.Signal(syscall.SIGTERM); termErr != nil {
				tracelog.ErrorLogger.Printf("failed to send SIGTERM to the command process: %v", termErr)
			}
			return fmt.Errorf("command terminated")
		}
	}
}

// TODO: unit tests
func checkCmdStatus(ticker *time.Ticker, doneCh chan error, sigCh chan os.Signal) (SegCmdStatus, error) {
	select {
	case <-ticker.C:
		tracelog.DebugLogger.Printf("Tick")
		return RunningCmdStatus, nil

	case err := <-doneCh:
		if err != nil {
			return FailedCmdStatus, err
		}

		return SuccessCmdStatus, nil

	case sig := <-sigCh:
		tracelog.ErrorLogger.Printf("Received signal: %s, terminating the running command...", sig)
		return InterruptedCmdStatus, nil
	}
}

func writeCmdState(state SegCmdState, contentID int, backupName string) error {
	statePath := FormatCmdStatePath(contentID, backupName)
	dstFile, err := os.OpenFile(statePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
	if err != nil {
		return fmt.Errorf("failed to open the command state file: %v", err)
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
