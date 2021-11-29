package pg

import (
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const UseSentinelTimeFlag = "use-sentinel-time"
const UseSentinelTimeDescription = "Use backup creation time from sentinel for backups ordering."

var confirmed = false
var useSentinelTime = false
var deleteTargetUserData = ""

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: internal.DeleteShortDescription, // TODO : improve description
}

var deleteBeforeCmd = &cobra.Command{
	Use:     internal.DeleteBeforeUsageExample, // TODO : improve description
	Example: internal.DeleteBeforeExamples,
	Args:    internal.DeleteBeforeArgsValidator,
	Run:     runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       internal.DeleteRetainUsageExample, // TODO : improve description
	Example:   internal.DeleteRetainExamples,
	ValidArgs: internal.StringModifiers,
	Run:       runDeleteRetain,
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample, // TODO : improve description
	Example:   internal.DeleteEverythingExamples,
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

var deleteTargetCmd = &cobra.Command{
	Use:     internal.DeleteTargetUsageExample, // TODO : improve description
	Example: internal.DeleteTargetExamples,
	Args:    internal.DeleteTargetArgsValidator,
	Run:     runDeleteTarget,
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, useSentinelTime)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, useSentinelTime)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args, confirmed)
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, useSentinelTime)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteEverything(args, permanentBackups, confirmed)
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	findFullBackup := false
	modifier := internal.ExtractDeleteTargetModifierFromArgs(args)
	if modifier == internal.FindFullDeleteModifier {
		findFullBackup = true
		// remove the extracted modifier from args
		args = args[1:]
	}

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, useSentinelTime)
	tracelog.ErrorLogger.FatalOnError(err)
	targetBackupSelector, err := internal.CreateTargetDeleteBackupSelector(cmd, args, deleteTargetUserData, postgres.NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler.HandleDeleteTarget(targetBackupSelector, confirmed, findFullBackup)
}

func init() {
	Cmd.AddCommand(deleteCmd)

	deleteTargetCmd.Flags().StringVar(
		&deleteTargetUserData, internal.DeleteTargetUserDataFlag, "", internal.DeleteTargetUserDataDescription)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd, deleteTargetCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
	deleteCmd.PersistentFlags().BoolVar(&useSentinelTime, UseSentinelTimeFlag, false, UseSentinelTimeDescription)
}
