package etcd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/etcd"
)

var (
	copyBackupName string
	copyFrom       string
	copyTo         string
)

var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a backup to another storage without transforming payloads",
	Args:  cobra.NoArgs,
	Run: func(command *cobra.Command, _ []string) {
		etcd.HandleCopy(command.Context(), copyFrom, copyTo, copyBackupName)
	},
	PersistentPreRun: func(*cobra.Command, []string) {},
}

func init() {
	copyCmd.Flags().StringVarP(&copyBackupName, "backup-name", "b", "", "copy one backup (or LATEST); empty copies all")
	copyCmd.Flags().StringVarP(&copyFrom, "from", "f", "", "source storage configuration file")
	copyCmd.Flags().StringVarP(&copyTo, "to", "t", "", "destination storage configuration file")
	_ = copyCmd.MarkFlagRequired("from")
	_ = copyCmd.MarkFlagRequired("to")
	cmd.AddCommand(copyCmd)
}
