package mongo

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	mongodb "github.com/wal-g/wal-g/internal/databases/mongo"
)

var (
	copyOutput      string
	copyOutputFile  string
	copyBackupName  string
	copyFrom        string
	copyTo          string
	copyWithHistory bool
)

var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a MongoDB backup and optionally synchronize oplog history",
	Args:  cobra.NoArgs,
	Run: func(command *cobra.Command, _ []string) {
		if command.Flags().Changed("output") {
			if copyOutput != "rclone" {
				tracelog.ErrorLogger.Fatalf("unsupported --output %q: expected rclone", copyOutput)
			}
			if copyOutputFile == "" {
				tracelog.ErrorLogger.Fatalf("--output-file must not be empty")
			}
			from, _, err := internal.StorageAndConfigFromFile(command.Context(), copyFrom)
			tracelog.ErrorLogger.FatalOnError(err)
			plan, err := mongodb.BuildCopyPlan(command.Context(), from.RootFolder(), nil, copyBackupName, copyWithHistory)
			tracelog.ErrorLogger.FatalOnError(err)
			if err := plan.WriteRcloneFileList(copyOutputFile); err != nil {
				tracelog.ErrorLogger.Fatalf("write rclone file list %q: %v", copyOutputFile, err)
			}
			tracelog.InfoLogger.Printf("Wrote rclone file list to %q.", copyOutputFile)
			return
		}
		mongodb.HandleCopy(command.Context(), copyFrom, copyTo, copyBackupName, copyWithHistory)
	},
	PersistentPreRun: func(*cobra.Command, []string) {},
}

func init() {
	copyCmd.Flags().StringVarP(&copyBackupName, "backup-name", "b", "", "copy one backup (or LATEST); empty copies all")
	copyCmd.Flags().StringVarP(&copyFrom, "from", "f", "", "source storage configuration file")
	copyCmd.Flags().StringVarP(&copyTo, "to", "t", "", "destination storage configuration file")
	copyCmd.Flags().BoolVarP(&copyWithHistory, "with-history", "w", false, "synchronize oplog history through the latest archived entry")
	_ = copyCmd.MarkFlagRequired("from")
	copyCmd.Flags().StringVar(&copyOutput, "output", "", "write a file list instead of copying: rclone")
	copyCmd.Flags().StringVar(&copyOutputFile, "output-file", "", "path to the output file list")
	copyCmd.MarkFlagsRequiredTogether("output", "output-file")
	copyCmd.MarkFlagsOneRequired("to", "output")
	cmd.AddCommand(copyCmd)
}
