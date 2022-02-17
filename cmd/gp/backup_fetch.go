package gp

import (
	"fmt"

	"github.com/wal-g/wal-g/internal/databases/greenplum"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupFetchShortDescription  = "Fetches a backup from storage"
	targetUserDataDescription    = "Fetch storage backup which has the specified user data"
	restorePointDescription      = "Fetch storage backup w/ restore point specified by name"
	restoreConfigPathDescription = "Path to the cluster restore configuration"
	fetchContentIdsDescription   = "If set, WAL-G will fetch only the specified segments"
	fetchModeDescription         = "Backup fetch mode. default: do the backup unpacking " +
		"and prepare the configs [unpack+prepare], unpack: backup unpacking only, prepare: config preparation only."
	inPlaceFlagDescription = "Perform the backup fetch in-place (without the restore config)"
)

var fetchTargetUserData string
var restorePoint string
var restoreConfigPath string
var fetchContentIds *[]int
var fetchModeStr string
var inPlaceRestore bool

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch [backup_name | --target-user-data <data> | --restore-point <name>]",
	Short: backupFetchShortDescription, // TODO : improve description
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		if !inPlaceRestore && restoreConfigPath == "" {
			tracelog.ErrorLogger.Fatalf(
				"No restore config was specified. Either specify one via the --restore-config flag or add the --in-place flag to restore in-place.")
		}

		if fetchTargetUserData == "" {
			fetchTargetUserData = viper.GetString(internal.FetchTargetUserDataSetting)
		}
		targetBackupSelector, err := createTargetFetchBackupSelector(cmd, args, fetchTargetUserData, restorePoint)
		tracelog.ErrorLogger.FatalOnError(err)

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		logsDir := viper.GetString(internal.GPLogsDirectory)

		if len(*fetchContentIds) > 0 {
			tracelog.InfoLogger.Printf("Will perform fetch operations only on the specified segments: %v", *fetchContentIds)
		}

		fetchMode, err := greenplum.NewBackupFetchMode(fetchModeStr)
		tracelog.ErrorLogger.FatalOnError(err)

		internal.HandleBackupFetch(folder, targetBackupSelector,
			greenplum.NewGreenplumBackupFetcher(restoreConfigPath, inPlaceRestore, logsDir, *fetchContentIds, fetchMode, restorePoint))
	},
}

// create the BackupSelector to select the backup to fetch
func createTargetFetchBackupSelector(cmd *cobra.Command,
	args []string, targetUserData, restorePoint string) (internal.BackupSelector, error) {
	targetName := ""
	if len(args) >= 1 {
		targetName = args[0]
	}

	// if target restore point is provided without the backup name, then
	// choose the latest backup up to the specified restore point name
	if restorePoint != "" && targetUserData == "" && len(args) == 0 {
		tracelog.InfoLogger.Printf("Restore point %s is specified without the backup name or target user data, "+
			"will search for a matching backup", restorePoint)
		return greenplum.NewRestorePointBackupSelector(restorePoint), nil
	}

	backupSelector, err := internal.NewTargetBackupSelector(targetUserData, targetName, greenplum.NewGenericMetaFetcher())
	if err != nil {
		fmt.Println(cmd.UsageString())
		return nil, err
	}
	return backupSelector, nil
}

func init() {
	backupFetchCmd.Flags().StringVar(&fetchTargetUserData, "target-user-data",
		"", targetUserDataDescription)
	backupFetchCmd.Flags().StringVar(&restorePoint, "restore-point", "", restorePointDescription)
	backupFetchCmd.Flags().StringVar(&restoreConfigPath, "restore-config",
		"", restoreConfigPathDescription)
	backupFetchCmd.Flags().BoolVar(&inPlaceRestore, "in-place", false, inPlaceFlagDescription)
	fetchContentIds = backupFetchCmd.Flags().IntSlice("content-ids", []int{}, fetchContentIdsDescription)

	backupFetchCmd.Flags().StringVar(&fetchModeStr, "mode", "default", fetchModeDescription)
	cmd.AddCommand(backupFetchCmd)
}
