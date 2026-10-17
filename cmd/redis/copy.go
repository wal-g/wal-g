package redis

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
)

var (
	copyOutput     string
	copyOutputFile string
	copyBackupName string
	copyFrom       string
	copyTo         string
)

var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a Redis or Valkey backup without transforming payloads",
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
			plan, err := redisdb.BuildCopyPlan(command.Context(), from.RootFolder(), nil, copyBackupName)
			tracelog.ErrorLogger.FatalOnError(err)
			if err := plan.WriteRcloneFileList(copyOutputFile); err != nil {
				tracelog.ErrorLogger.Fatalf("write rclone file list %q: %v", copyOutputFile, err)
			}
			tracelog.InfoLogger.Printf("Wrote rclone file list to %q.", copyOutputFile)
			return
		}
		redisdb.HandleCopy(command.Context(), copyFrom, copyTo, copyBackupName)
	},
	PersistentPreRun: func(*cobra.Command, []string) {},
}

func init() {
	copyCmd.Flags().StringVarP(&copyBackupName, "backup-name", "b", "", "copy one backup (or LATEST); empty copies all")
	copyCmd.Flags().StringVarP(&copyFrom, "from", "f", "", "source storage configuration file")
	copyCmd.Flags().StringVarP(&copyTo, "to", "t", "", "destination storage configuration file")
	_ = copyCmd.MarkFlagRequired("from")
	copyCmd.Flags().StringVar(&copyOutput, "output", "", "write a file list instead of copying: rclone")
	copyCmd.Flags().StringVar(&copyOutputFile, "output-file", "", "path to the output file list")
	copyCmd.MarkFlagsRequiredTogether("output", "output-file")
	copyCmd.MarkFlagsOneRequired("to", "output")
	cmd.AddCommand(copyCmd)
}
