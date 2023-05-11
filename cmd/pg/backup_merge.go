package pg

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

var targetIncrementalBackupName string

const (
	backupMergeShortDescription            = "Create a single backup from delta backups and put it in storage"
	targetIncrementalBackupNameDescription = "Name of the target delta backup relative to which the base backup should be generated"
)

var backupMergeCmd = &cobra.Command{
	Use:   "backup-merge backup_name",
	Short: backupMergeShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// TODO checks args (backup name should be exists and name must be as delta backup)
		targetBackupName := args[0]
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		composer := chooseTarBallComposer2()

		mergeHandler, err := postgres.NewBackupMergeHandler(targetBackupName, folder, composer)
		tracelog.ErrorLogger.FatalOnError(err)

		mergeHandler.HandleBackupMerge()
		tracelog.InfoLogger.Println("DONE")
	},
}

// copy from backup_push
func chooseTarBallComposer2() postgres.TarBallComposerType {
	tarBallComposerType := postgres.RegularComposer

	useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)
	if useRatingComposer {
		tarBallComposerType = postgres.RatingComposer
	}

	useDatabaseComposer = useDatabaseComposer || viper.GetBool(internal.UseDatabaseComposerSetting)
	if useDatabaseComposer {
		tarBallComposerType = postgres.DatabaseComposer
	}

	useCopyComposer = useCopyComposer || viper.GetBool(internal.UseCopyComposerSetting)
	if useCopyComposer {
		fullBackup = true
		tarBallComposerType = postgres.CopyComposer
	}

	return tarBallComposerType
}

func init() {
	// TODO add flags as backup-fetch
	backupMergeCmd.Flags().StringVar(&targetIncrementalBackupName, "target-backup-name", "",
		targetIncrementalBackupNameDescription)

	Cmd.AddCommand(backupMergeCmd)
}
