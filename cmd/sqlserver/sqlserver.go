package sqlserver

import (
	"fmt"
	"os"
	"strings"

	"github.com/wal-g/wal-g/cmd/common"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
)

var ShortDescription = "SQLServer backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var walgVersion = "devel"
var gitRevision = "devel"
var buildDate = "devel"

var cmd = &cobra.Command{
	Use:     "sqlserver",
	Short:   ShortDescription,
	Version: strings.Join([]string{walgVersion, gitRevision, buildDate, "SQLServer"}, "\t"),
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main().
func Execute() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	common.Init(cmd, internal.SQLSERVER)
}
