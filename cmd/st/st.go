package st

import (
	"github.com/spf13/cobra"
)

// Storage tools allows to interact with the configured storage, e.g.:
// - get the raw file listings,
// - download/upload files from/to storage (TODO)
// - delete/move/copy storage files (TODO)
// - ...
// Be aware that these commands can do potentially harmful operations and make sure that you know what you're doing.

const StorageToolsShortDescription = "(DANGEROUS) Storage tools"

var (
	StorageToolsCmd = &cobra.Command{
		Use:   "st",
		Short: StorageToolsShortDescription,
		Long: "Storage tools allows to interact with the configured storage. " +
			"Be aware that this command can do potentially harmful operations and make sure that you know what you're doing.",
	}
)
