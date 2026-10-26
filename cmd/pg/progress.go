package pg

import (
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/progress"
	"golang.org/x/term"
)

const progressFlag = "progress"

func startProgress(cmd *cobra.Command) func() {
	enabled, _ := cmd.Flags().GetBool(progressFlag)
	output, isFile := cmd.ErrOrStderr().(*os.File)
	if !enabled || !isFile || os.Getenv("TERM") == "dumb" || !term.IsTerminal(int(output.Fd())) {
		return func() {}
	}
	ctx := cmd.Context()
	tracker := progress.Start(ctx, output, cmd.Name(), 0)
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
		if logger.Writer() == output {
			logger.SetOutput(tracker)
			redirected = append(redirected, logger)
		}
	}
	return func() {
		tracker.Stop()
		for _, logger := range redirected {
			logger.SetOutput(output)
		}
		cmd.SetContext(ctx)
	}
}

func addProgressFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(progressFlag, true, "Show live transfer progress on an interactive stderr terminal")
}
