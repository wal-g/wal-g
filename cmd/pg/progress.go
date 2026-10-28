package pg

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/progress"
	"golang.org/x/term"
)

const (
	progressFlag          = "progress"
	progressDirectoryFlag = "progress-dir"
	progressRetentionFlag = "progress-retention"
)

func startProgress(cmd *cobra.Command) func() {
	enabled, _ := cmd.Flags().GetBool(progressFlag)
	output, isFile := cmd.ErrOrStderr().(*os.File)
	interactive := enabled && isFile && os.Getenv("TERM") != "dumb" && term.IsTerminal(int(output.Fd()))
	directory, _ := cmd.Flags().GetString(progressDirectoryFlag)
	retention, _ := cmd.Flags().GetDuration(progressRetentionFlag)
	if !interactive && directory == "" {
		return func() {}
	}
	var terminalOutput io.Writer
	if interactive {
		terminalOutput = output
	}
	ctx := cmd.Context()
	tracker := progress.StartWithFile(ctx, terminalOutput, cmd.Name(), 0, progress.FileOptions{
		Directory: directory, Retention: retention,
		OnError: func(err error) { tracelog.WarningLogger.Printf("Progress file reporting failed: %v", err) },
	})
	cmd.SetContext(progress.WithTracker(ctx, tracker))
	progress.SetStage(cmd.Context(), "preparing")

	// Only intercept loggers writing to this terminal. Respect file logging and
	// disabled levels, whose writer is io.Discard.
	loggers := []*log.Logger{
		tracelog.InfoLogger.Logger, tracelog.WarningLogger.Logger,
		tracelog.ErrorLogger.Logger, tracelog.DebugLogger.Logger,
	}
	var redirected []*log.Logger
	for _, logger := range loggers {
		if interactive && logger.Writer() == output {
			logger.SetOutput(tracker)
			redirected = append(redirected, logger)
		}
	}
	return func() {
		panicValue := recover()
		if panicValue != nil {
			tracker.StopOnFailure()
		} else {
			tracker.Stop()
		}
		for _, logger := range redirected {
			logger.SetOutput(output)
		}
		cmd.SetContext(ctx)
		if panicValue != nil {
			panic(panicValue)
		}
	}
}

func addProgressFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(progressFlag, true, "Show live transfer progress on an interactive stderr terminal")
	cmd.Flags().String(progressDirectoryFlag, "", "Write a separate JSON progress snapshot per run in this directory")
	cmd.Flags().Duration(progressRetentionFlag, 24*time.Hour, "Remove completed progress snapshots older than this at startup (nonpositive disables cleanup)")
}
