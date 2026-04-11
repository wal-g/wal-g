package etcd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/etcd"
	"github.com/wal-g/wal-g/internal/logging"
)

var confirmed = false
var deleteTargetUserData = ""

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: internal.DeleteShortDescription,
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
	Use:       internal.DeleteEverythingUsageExample,
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
	storage, err := internal.ConfigureStorage()
	logging.FatalOnError(err)

	deleteHandler, err := etcd.NewEtcdDeleteHandler(storage.RootFolder())
	logging.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	storage, err := internal.ConfigureStorage()
	logging.FatalOnError(err)

	deleteHandler, err := etcd.NewEtcdDeleteHandler(storage.RootFolder())
	logging.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args, confirmed)
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	storage, err := internal.ConfigureStorage()
	logging.FatalOnError(err)

	deleteHandler, err := etcd.NewEtcdDeleteHandler(storage.RootFolder())
	logging.FatalOnError(err)

	deleteHandler.DeleteEverything(confirmed)
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	storage, err := internal.ConfigureStorage()
	logging.FatalOnError(err)

	deleteHandler, err := etcd.NewEtcdDeleteHandler(storage.RootFolder())
	logging.FatalOnError(err)
	targetBackupSelector, err := internal.CreateTargetDeleteBackupSelector(cmd, args, deleteTargetUserData, etcd.NewGenericMetaFetcher())
	logging.FatalOnError(err)

	deleteHandler.HandleDeleteTarget(targetBackupSelector, confirmed, false)
}

func init() {
	cmd.AddCommand(deleteCmd)

	deleteTargetCmd.Flags().StringVar(
		&deleteTargetUserData, internal.DeleteTargetUserDataFlag, "", internal.DeleteTargetUserDataDescription)

	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd, deleteEverythingCmd, deleteTargetCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}
