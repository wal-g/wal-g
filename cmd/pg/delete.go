package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const UseSentinelTimeFlag = "use-sentinel-time"
const UseSentinelTimeDescription = "Use backup creation time from sentinel for backups ordering."
const DeleteGarbageExamples = `  garbage           Deletes outdated WAL archives and leftover backups files from storage
  garbage ARCHIVES  Deletes only outdated WAL archives from storage
  garbage BACKUPS   Deletes only leftover backups files from storage`
const DeleteGarbageUse = "garbage [ARCHIVES|BACKUPS]"
const afterFlag = "after"

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
	Args:      internal.DeleteRetainArgsValidator,
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
	Args:    DeleteGarbageArgsValidator,
	Run:     runDeleteGarbage,
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	folder := configureFolder()

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, useSentinelTime)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder := configureFolder()

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, useSentinelTime)
	tracelog.ErrorLogger.FatalOnError(err)

	afterValue, _ := cmd.Flags().GetString(afterFlag)
	if afterValue == "" {
		deleteHandler.HandleDeleteRetain(args, confirmed)
	} else {
		deleteHandler.HandleDeleteRetainAfter(append(args, afterValue), confirmed)
	}
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder := configureFolder()

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, useSentinelTime)
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackupNames := make([]string, 0, len(permanentBackups))
	for backup, isPerm := range permanentBackups {
		if isPerm {
			permanentBackupNames = append(permanentBackupNames, backup.Name)
		}
	}
	deleteHandler.HandleDeleteEverything(args, permanentBackupNames, confirmed)
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	folder := configureFolder()

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

func runDeleteGarbage(cmd *cobra.Command, args []string) {
	folder := configureFolder()

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, false)
	tracelog.ErrorLogger.FatalOnError(err)

	err = deleteHandler.HandleDeleteGarbage(args, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func configureFolder() storage.Folder {
	multiSt, err := internal.ConfigureMultiStorage(true)
	tracelog.ErrorLogger.FatalfOnError("Failed to configure multi-storage: %v", err)

	rootFolder, err := multistorage.UseAllAliveStorages(multiSt.RootFolder())
	tracelog.InfoLogger.Printf("Backup to delete will be searched in storages: %v", multistorage.UsedStorages(rootFolder))
	tracelog.ErrorLogger.FatalOnError(err)
	return multistorage.SetPolicies(rootFolder, policies.UniteAllStorages)
}

func DeleteGarbageArgsValidator(cmd *cobra.Command, args []string) error {
	modifiers := []string{postgres.DeleteGarbageArchivesModifier, postgres.DeleteGarbageBackupsModifier}
	return internal.DeleteArgsValidator(args, modifiers, 0, 1)
}

func init() {
	Cmd.AddCommand(deleteCmd)

	deleteTargetCmd.Flags().StringVar(
		&deleteTargetUserData, internal.DeleteTargetUserDataFlag, "", internal.DeleteTargetUserDataDescription)
	deleteRetainCmd.Flags().StringP(afterFlag, "a", "", "Set the time after which retain backups")

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd, deleteTargetCmd, deleteGarbageCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
	deleteCmd.PersistentFlags().BoolVar(&useSentinelTime, UseSentinelTimeFlag, false, UseSentinelTimeDescription)
}
