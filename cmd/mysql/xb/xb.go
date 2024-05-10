package xb

import (
	"github.com/spf13/cobra"
)

// low level xbstream/xtrabackup tools that allow to work with local backups:

const xbToolsShortDescription = "(DANGEROUS) xbstream tools"
const xbToolsLongDescription = "xbstream tools allows to interact with local xbstream backups. " +
	"Be aware that this command can do potentially harmful operations and make sure that you know what you're doing."

var (
	XBToolsCmd = &cobra.Command{
		Use:   "xb",
		Short: xbToolsShortDescription,
		Long:  xbToolsLongDescription,
	}
)
