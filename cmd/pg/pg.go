package pg

import (
	"fmt"
	"os"
	"strings"

	"github.com/wal-g/wal-g/internal"

	"github.com/spf13/cobra"
)

const WalgShortDescription = "PostgreSQL backup tool"

var (
	// These variables are here only to show current version. They are set in makefile during build process
	WalgVersion = "devel"
	GitRevision = "devel"
	BuildDate   = "devel"

	Cmd = &cobra.Command{
		Use:     "wal-g",
		Short:   WalgShortDescription, // TODO : improve short and long descriptions
		Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "PostgreSQL"}, "\t"),
	}
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the PgCmd.
func Execute() {
	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure, internal.AssertRequiredSettingsSet)

	Cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	Cmd.InitDefaultVersionFlag()
	internal.AddConfigFlags(Cmd)
}
