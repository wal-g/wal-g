package gp

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/wal-g/cmd/common"

	"github.com/wal-g/wal-g/cmd/pg"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

var dbShortDescription = "GreenplumDB backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var walgVersion = "devel"
var gitRevision = "devel"
var buildDate = "devel"

var cmd = &cobra.Command{
	Use:     "wal-g",
	Short:   dbShortDescription, // TODO : improve description
	Version: strings.Join([]string{walgVersion, gitRevision, buildDate, "GreenplumDB"}, "\t"),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Greenplum uses the 64MB WAL segment size by default
		postgres.SetWalSize(viper.GetUint64(internal.PgWalSize))
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
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
	common.Init(cmd, internal.GP)

	_ = cmd.MarkFlagRequired("config") // config is required for Greenplum WAL-G

	// wrap the Postgres command so it can be used in the same binary
	wrappedPgCmd := pg.Cmd
	wrappedPgCmd.Use = "pg"
	wrappedPreRun := wrappedPgCmd.PersistentPreRun
	wrappedPgCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		// storage prefix setting is required in order to get the corresponding segment subfolder
		internal.RequiredSettings[internal.StoragePrefixSetting] = true
		wrappedPreRun(cmd, args)
	}
	cmd.AddCommand(wrappedPgCmd)

	// Add the hidden prefetch command to the root command since there is no "pg" prefix in the WAL-G prefetch fork logic
	pg.WalPrefetchCmd.PreRun = func(cmd *cobra.Command, args []string) {
		internal.RequiredSettings[internal.StoragePrefixSetting] = true
		tracelog.ErrorLogger.FatalOnError(internal.AssertRequiredSettingsSet())
	}
	cmd.AddCommand(pg.WalPrefetchCmd)
}
