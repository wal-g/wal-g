package pg

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/cmd/common"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const WalgShortDescription = "PostgreSQL backup tool"

var (
	// These variables are here only to show current version. They are set in makefile during build process
	walgVersion = "devel"
	gitRevision = "devel"
	buildDate   = "devel"

	Cmd = &cobra.Command{
		Use:     "wal-g",
		Short:   WalgShortDescription, // TODO : improve short and long descriptions
		Version: strings.Join([]string{walgVersion, gitRevision, buildDate, "PostgreSQL"}, "\t"),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if _, ok := cmd.Annotations["NoStorage"]; !ok {
				err := internal.AssertRequiredSettingsSet()
				tracelog.ErrorLogger.FatalOnError(err)
			}

			if viper.IsSet(conf.PgWalSize) {
				postgres.SetWalSize(viper.GetUint64(conf.PgWalSize))
			}

			// In case the --target-storage flag isn't specified (the variable is set in commands' init() funcs),
			// we take the value from the config.
			if targetStorage == "" {
				targetStorage = viper.GetString(conf.PgTargetStorage)
			}
		},
	}

	targetStorage            string
	targetStorageDescription = `Name of the storage to execute the command only for. Use "default" to select the primary one.`
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the PgCmd.
func Execute() {
	configureCommand()
	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func GetCmd() *cobra.Command {
	return Cmd
}

func configureCommand() {
	common.Init(Cmd, conf.PG)
	conf.AddTurboFlag(Cmd)
}
