package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
)

var confirmed = false
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

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed}
	deleteHandler, err := greenplum.NewDeleteHandler(folder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed}
	deleteHandler, err := greenplum.NewDeleteHandler(folder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args)
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed}
	deleteHandler, err := greenplum.NewDeleteHandler(folder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteEverything(args)
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	findFullBackup := false
	modifier := internal.ExtractDeleteTargetModifierFromArgs(args)
	if modifier == internal.FindFullDeleteModifier {
		findFullBackup = true
		// remove the extracted modifier from args
		args = args[1:]
	}

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed, FindFull: findFullBackup}
	deleteHandler, err := greenplum.NewDeleteHandler(folder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	targetBackupSelector, err := internal.CreateTargetDeleteBackupSelector(
		cmd, args, deleteTargetUserData, greenplum.NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)

	target := deleteHandler.FindTargetBySelector(targetBackupSelector)
	deleteHandler.DeleteTarget(target)
}

func init() {
	cmd.AddCommand(deleteCmd)

	deleteTargetCmd.Flags().StringVar(
		&deleteTargetUserData, internal.DeleteTargetUserDataFlag, "", internal.DeleteTargetUserDataDescription)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd, deleteTargetCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}
