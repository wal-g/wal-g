package rocksdb

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	// "github.com/wal-g/wal-g/utility"
)

const (
	retainAfterFlag = "retain-after"
	retainCountFlag = "retain-count"
)

var (
	retainAfter string
	retainCount uint
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete old backups",
	Run:   runDelete,
}

func runDelete(cmd *cobra.Command, args []string) {
	internal.ConfigureFolder()
}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteCmd.Flags().StringVar(&retainAfter, retainAfterFlag, "", "Keep backups newer")
	deleteCmd.Flags().UintVar(&retainCount, retainCountFlag, 0, "Keep minimum count")
}
