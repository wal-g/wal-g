package gp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/wal-g/wal-g/internal/databases/greenplum"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupFetchShortDescription  = "Fetches a backup from storage"
	targetUserDataDescription    = "Fetch storage backup which has the specified user data"
	restoreConfigPathDescription = "Path to the cluster restore configuration"
	fetchContentIdsDescription   = "If set, WAL-G will fetch only the specified segments"
	fetchModeDescription         = "Backup fetch mode. default: do the backup unpacking " +
		"and prepare the configs [unpack+prepare], unpack: backup unpacking only, prepare: config preparation only."
)

var fetchTargetUserData string
var restoreConfigPath string
var fetchContentIds *[]int
var fetchModeStr string

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch [backup_name | --target-user-data <data>]",
	Short: backupFetchShortDescription, // TODO : improve description
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		if fetchTargetUserData == "" {
			fetchTargetUserData = viper.GetString(internal.FetchTargetUserDataSetting)
		}
		targetBackupSelector, err := createTargetFetchBackupSelector(cmd, args, fetchTargetUserData)
		tracelog.ErrorLogger.FatalOnError(err)

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		file, err := ioutil.ReadFile(restoreConfigPath)
		tracelog.ErrorLogger.FatalfOnError("Failed to open the provided restore config file: %v", err)

		var restoreCfg greenplum.ClusterRestoreConfig
		err = json.Unmarshal(file, &restoreCfg)
		tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided restore config file: %v", err)

		logsDir := viper.GetString(internal.GPLogsDirectory)

		if len(*fetchContentIds) > 0 {
			tracelog.InfoLogger.Printf("Will perform fetch operations only on the specified segments: %v", *fetchContentIds)
		}

		fetchMode, err := greenplum.NewBackupFetchMode(fetchModeStr)
		tracelog.ErrorLogger.FatalOnError(err)

		internal.HandleBackupFetch(folder, targetBackupSelector,
			greenplum.NewGreenplumBackupFetcher(restoreCfg, logsDir, *fetchContentIds, fetchMode))
	},
}

// create the BackupSelector to select the backup to fetch
func createTargetFetchBackupSelector(cmd *cobra.Command,
	args []string, targetUserData string) (internal.BackupSelector, error) {
	targetName := ""
	if len(args) >= 1 {
		targetName = args[0]
	}

	backupSelector, err := internal.NewTargetBackupSelector(targetUserData, targetName, postgres.NewGenericMetaFetcher())
	if err != nil {
		fmt.Println(cmd.UsageString())
		return nil, err
	}
	return backupSelector, nil
}

func init() {
	backupFetchCmd.Flags().StringVar(&fetchTargetUserData, "target-user-data",
		"", targetUserDataDescription)
	backupFetchCmd.Flags().StringVar(&restoreConfigPath, "restore-config",
		"", restoreConfigPathDescription)
	fetchContentIds = backupFetchCmd.Flags().IntSlice("content-ids", []int{}, fetchContentIdsDescription)
	_ = backupFetchCmd.MarkFlagRequired("restore-config")

	backupFetchCmd.Flags().StringVar(&fetchModeStr, "mode", "default", fetchModeDescription)
	cmd.AddCommand(backupFetchCmd)
}
