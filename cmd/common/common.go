package common

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/cmd/common/st"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/statistics"
)

const usageTemplate = `Usage:{{if .Runnable}}
{{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
{{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
{{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
{{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}` +
	// Config flags are hidden by default
	`

To get the complete list of all global flags, run: 'wal-g flags'` +
	`{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
{{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

func Init(cmd *cobra.Command, dbName string) {
	internal.ConfigureSettings(dbName)
	cobra.OnInitialize(conf.InitConfig, conf.Configure)

	cmd.InitDefaultVersionFlag()
	configFlags := conf.AddConfigFlags(cmd)

	cmd.PersistentFlags().StringVar(
		&conf.CfgFile,
		"config",
		"",
		"config file (default is $HOME/.walg.json, can also be set via WALG_CONFIG_PATH env var)",
	)

	initHelp(cmd, configFlags)

	// Add flags subcommand
	cmd.AddCommand(FlagsCmd)

	// Add completion subcommand
	cmd.AddCommand(CompletionCmd)

	// Add storage tools
	cmd.AddCommand(st.StorageToolsCmd)

	// profiler
	persistentPreRun := cmd.PersistentPreRun
	persistentPostRun := cmd.PersistentPostRun

	var p internal.ProfileStopper
	cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if persistentPreRun != nil {
			persistentPreRun(cmd, args)
		}

		var err error
		p, err = internal.Profile()
		tracelog.ErrorLogger.FatalOnError(err)
	}
	cmd.PersistentPostRun = func(cmd *cobra.Command, args []string) {
		if persistentPostRun != nil {
			persistentPostRun(cmd, args)
		}

		// metrics hook
		statistics.PushMetrics()

		if p != nil {
			p.Stop()
		}
	}

	// Don't run PersistentPreRun when shell autocompleting
	preRun := cmd.PersistentPreRun
	cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if strings.Index(cmd.Use, cobra.ShellCompRequestCmd) == 0 {
			return
		}
		preRun(cmd, args)
	}
}

// setup init and usage functionality
func initHelp(cmd *cobra.Command, configFlags *pflag.FlagSet) {
	cmd.SetUsageTemplate(usageTemplate)
	defaultUsageFn := (&cobra.Command{}).UsageFunc()
	defaultHelpFn := (&cobra.Command{}).HelpFunc()

	// Keep global config flags hidden except when displaying the "flags" command.
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		if cmd == FlagsCmd {
			configFlags.VisitAll(func(f *pflag.Flag) {
				f.Hidden = false
			})
			defer configFlags.VisitAll(func(f *pflag.Flag) {
				f.Hidden = true
			})
		}
		return defaultUsageFn(cmd)
	})

	// hide global config flags from help output
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		defaultHelpFn(cmd, args)
	})

	// Init help subcommand
	cmd.InitDefaultHelpCmd()
	helpCmd, _, _ := cmd.Find([]string{"help"})
	// fix to disable the required settings check for the help subcommand
	helpCmd.PersistentPreRun = func(*cobra.Command, []string) {}
}
