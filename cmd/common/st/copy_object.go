package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/storagetools"
)

const (
	copyObjectShortDescription = "Copy objects from one storage to another"


	fromFlag        = "from"
	fromShorthand   = "f"
	fromDescription = "Storage config from where should copy objects"

	toFlag        = "to"
	toShorthand   = "t"
	toDescription = "Storage config to where should copy objects"


	prefixFlag        = "prefix"
	prefixShorthand   = "p"
	prefixDescription = "Prefix-filter path in `from` storage."


	decryptSourceFlag        = "decrypt-source"
	decryptSourceShorthand   = "d"
	decryptSourceDescription = "Decrypt file from source storage"

	encryptTargetFlag        = "encrypt-source"
	encryptTargetShorthand   = "e"
	encryptTargetDescription = "Encypt file in target storage"
)

var (
	fromConfigFile string
	toConfigFile   string
	prefix string

	decryptSource bool
	encryptTarget bool
)

// catObjectCmd represents the catObject command
var copyObjectCmd = &cobra.Command{
	Use:   "copy objects_path_prefix",
	Short: copyObjectShortDescription,
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		storagetools.HandleCopyObjects(fromConfigFile, toConfigFile, prefix, decryptSource, encryptTarget)
	},
	PersistentPreRun: func(*cobra.Command, []string) {
		// do not check for any configured settings because wal-g copy uses the different
		// settings init process
	},
}

func init() {
	copyObjectCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	copyObjectCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)
	copyObjectCmd.Flags().StringVarP(&prefix, prefixFlag, prefixShorthand, "", prefixDescription)

	copyObjectCmd.Flags().BoolVarP(&decryptSource, decryptSourceFlag, decryptSourceShorthand, false, decryptSourceDescription)
	copyObjectCmd.Flags().BoolVarP(&encryptTarget, encryptTargetFlag, encryptTargetShorthand, false, encryptTargetDescription)

	StorageToolsCmd.AddCommand(copyObjectCmd)
}
