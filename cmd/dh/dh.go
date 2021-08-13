package dh

import (
	"github.com/spf13/cobra"
)

// "Dirty hands" command allows to interact with the configured storage, e.g.:
// - get the raw file listings,
// - download/upload files from/to storage (TODO)
// - delete/move/copy storage files (TODO)
// - ...
// Be aware that this command can do potentially harmful operations and make sure that you know what you're doing.

const DirtyHandsShortDescription = "(DANGEROUS) Dirty hands tool"

var (
	DirtyHandsCmd = &cobra.Command{
		Use:   "dh",
		Short: DirtyHandsShortDescription,
		Long: "\"Dirty hands\" command allows to interact with the configured storage. " +
			"Be aware that this command can do potentially harmful operations and make sure that you know what you're doing.",
	}
)
