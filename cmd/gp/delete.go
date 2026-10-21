package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

var confirmed = false
var forceDelete = false

var deleteTargetUserData = ""

const DeleteGarbageExamples = `  garbage           Deletes outdated WAL archives and leftover backups files from storage`
const DeleteGarbageUse = "garbage"

const DeleteTrimWalUse = "trim-wal"
const DeleteTrimWalShortDescription = "Delete WAL files accumulated after the given backup's restore point"
const DeleteTrimWalExamples = "  trim-wal base_000000010000000000000001\n  trim-wal LATEST"

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

var deleteGarbageCmd = &cobra.Command{
	Use:     DeleteGarbageUse,
	Example: DeleteGarbageExamples,
	Args:    cobra.NoArgs,
	Run:     runDeleteGarbage,
}

var deleteTrimWalCmd = &cobra.Command{
	Use:     DeleteTrimWalUse + " BACKUP_NAME|LATEST",
	Short:   DeleteTrimWalShortDescription,
	Example: DeleteTrimWalExamples,
	Args:    cobra.ExactArgs(1),
	Run:     runDeleteTrimWal,
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	rootFolder, err := getMultistorageRootFolder(cmd.Context(), true, policies.UniteAllStorages)
	tracelog.ErrorLogger.FatalOnError(err)

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed, Force: forceDelete}
	deleteHandler, err := greenplum.NewDeleteHandler(cmd.Context(), rootFolder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(cmd.Context(), args)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	rootFolder, err := getMultistorageRootFolder(cmd.Context(), true, policies.UniteAllStorages)
	tracelog.ErrorLogger.FatalOnError(err)

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed, Force: forceDelete}
	deleteHandler, err := greenplum.NewDeleteHandler(cmd.Context(), rootFolder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(cmd.Context(), args)
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	rootFolder, err := getMultistorageRootFolder(cmd.Context(), true, policies.UniteAllStorages)
	tracelog.ErrorLogger.FatalOnError(err)

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed, Force: forceDelete}
	deleteHandler, err := greenplum.NewDeleteHandler(cmd.Context(), rootFolder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteEverything(cmd.Context(), args)
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	rootFolder, err := getMultistorageRootFolder(cmd.Context(), true, policies.UniteAllStorages)
	tracelog.ErrorLogger.FatalOnError(err)

	findFullBackup := false
	modifier := internal.ExtractDeleteTargetModifierFromArgs(args)
	if modifier == internal.FindFullDeleteModifier {
		findFullBackup = true
		// remove the extracted modifier from args
		args = args[1:]
	}

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed, FindFull: findFullBackup, Force: forceDelete}
	deleteHandler, err := greenplum.NewDeleteHandler(cmd.Context(), rootFolder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	targetBackupSelector, err := internal.CreateTargetDeleteBackupSelector(
		cmd, args, deleteTargetUserData, greenplum.NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteTarget(cmd.Context(), targetBackupSelector)
}

func runDeleteTrimWal(cmd *cobra.Command, args []string) {
	rootFolder, err := getMultistorageRootFolder(true, policies.UniteAllStorages)
	tracelog.ErrorLogger.FatalOnError(err)

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed}
	deleteHandler, err := greenplum.NewDeleteHandler(rootFolder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	err = deleteHandler.HandleDeleteTrimWal(cmd.Context(), args[0])
	tracelog.ErrorLogger.FatalOnError(err)
}

func runDeleteGarbage(cmd *cobra.Command, args []string) {
	rootFolder, err := getMultistorageRootFolder(cmd.Context(), true, policies.UniteAllStorages)
	tracelog.ErrorLogger.FatalOnError(err)

	delArgs := greenplum.DeleteArgs{Confirmed: confirmed, Force: true}
	deleteHandler, err := greenplum.NewDeleteHandler(cmd.Context(), rootFolder, delArgs)
	tracelog.ErrorLogger.FatalOnError(err)

	err = deleteHandler.HandleDeleteGarbage(cmd.Context(), args)
	tracelog.ErrorLogger.FatalOnError(err)
}

func init() {
	cmd.AddCommand(deleteCmd)

	deleteTargetCmd.Flags().StringVar(
		&deleteTargetUserData, internal.DeleteTargetUserDataFlag, "", internal.DeleteTargetUserDataDescription)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd, deleteTargetCmd, deleteGarbageCmd, deleteTrimWalCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
	deleteCmd.PersistentFlags().BoolVar(&forceDelete, "force-delete", false, "Force delete")
	_ = deleteCmd.PersistentFlags().MarkHidden("force-delete")
}
