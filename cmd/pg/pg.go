package pg

import (
	"fmt"
	"os"
	"strings"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const WalgShortDescription = "PostgreSQL backup tool"

var (
	// These variables are here only to show current version. They are set in makefile during build process
	walgVersion = "devel"
	gitRevision = "devel"
	buildDate   = "devel"

	cmd = &cobra.Command{
		Use:     "wal-g",
		Short:   WalgShortDescription, // TODO : improve short and long descriptions
		Version: strings.Join([]string{walgVersion, gitRevision, buildDate, "PostgreSQL"}, "\t"),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
			if viper.IsSet(internal.PgWalSize) {
				postgres.SetWalSize(viper.GetUint64(internal.PgWalSize))
			}
		},
	}
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the PgCmd.
func Execute() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	cmd.PersistentFlags().BoolVarP(&internal.Turbo, "turbo", "", false, "Ignore all kinds of throttling defined in config")
	cmd.InitDefaultVersionFlag()
	internal.AddConfigFlags(cmd)
}
