package pg

import (
	"fmt"
	"github.com/wal-g/wal-g/internal"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const WalgShortDescription = "PostgreSQL backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var PgCmd = &cobra.Command{
	Use:     "wal-g",
	Short:   WalgShortDescription, // TODO : improve short and long descriptions
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "PostgreSQL"}, "\t"),
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the PgCmd.
func Execute() {
	if err := PgCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	PgCmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	PgCmd.InitDefaultVersionFlag()
}
