package common

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// AddCopyOutputFlags adds a source-only file-list mode to a copy command.
func AddCopyOutputFlags(cmd *cobra.Command, buildPlan func(*cobra.Command, storage.Folder) (*copyutil.Plan, error)) {
	var output, outputFile string
	cmd.Flags().StringVar(&output, "output", "", "write a file list instead of copying: rclone")
	cmd.Flags().StringVar(&outputFile, "output-file", "", "path to the output file list")
	cmd.MarkFlagsRequiredTogether("output", "output-file")
	cmd.MarkFlagsOneRequired("to", "output")

	validateArgs := cmd.Args
	cmd.Args = func(command *cobra.Command, args []string) error {
		if err := validateArgs(command, args); err != nil {
			return err
		}
		if command.Flags().Changed("output") && output != "rclone" {
			return fmt.Errorf("unsupported --output %q: expected rclone", output)
		}
		if command.Flags().Changed("output-file") && outputFile == "" {
			return fmt.Errorf("--output-file must not be empty")
		}
		return nil
	}

	runCopy := cmd.Run
	cmd.Run = nil
	cmd.RunE = func(command *cobra.Command, args []string) error {
		if output == "" {
			runCopy(command, args)
			return nil
		}
		fromConfigFile, err := command.Flags().GetString("from")
		if err != nil {
			return err
		}
		from, _, err := internal.StorageAndConfigFromFile(command.Context(), fromConfigFile)
		if err != nil {
			return err
		}
		plan, err := buildPlan(command, from.RootFolder())
		if err != nil {
			return err
		}
		if err := plan.WriteRcloneFileList(outputFile); err != nil {
			return fmt.Errorf("write rclone file list %q: %w", outputFile, err)
		}
		tracelog.InfoLogger.Printf("Wrote rclone file list to %q.", outputFile)
		return nil
	}
}
