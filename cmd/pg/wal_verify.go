package pg

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalVerifyUsage            = "wal-verify"
	WalVerifyShortDescription = "Verify WAL storage folder. Available checks: integrity, timeline."
	WalVerifyLongDescription  = "Run a set of specified checks to ensure WAL storage health."

	useJSONOutputFlag        = "json"
	useJSONOutputDescription = "Show output in JSON format."

	checkIntegrityArg = "integrity"
	checkTimelineArg  = "timeline"
)

var (
	availableChecks = map[string]internal.WalVerifyCheckType{
		checkIntegrityArg: internal.WalVerifyIntegrityCheck,
		checkTimelineArg:  internal.WalVerifyTimelineCheck,
	}
	// walVerifyCmd represents the walVerify command
	walVerifyCmd = &cobra.Command{
		Use:   WalVerifyUsage,
		Short: WalVerifyShortDescription,
		Long:  WalVerifyLongDescription,
		Args:  checkArgs,
		Run: func(cmd *cobra.Command, checks []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			outputType := internal.WalVerifyTableOutput
			if useJSONOutput {
				outputType = internal.WalVerifyJSONOutput
			}
			outputWriter := internal.NewWalVerifyOutputWriter(outputType, os.Stdout)
			checkTypes := parseChecks(checks)

			internal.HandleWalVerify(checkTypes, folder, internal.QueryCurrentWalSegment(), outputWriter)
		},
	}
	useJSONOutput bool
)

func parseChecks(checks []string) []internal.WalVerifyCheckType {
	// filter the possible duplicates
	uniqueChecks := make(map[string]bool)
	for _, check := range checks {
		uniqueChecks[check] = true
	}

	checkTypes := make([]internal.WalVerifyCheckType, 0, len(checks))
	for check := range uniqueChecks {
		checkType, ok := availableChecks[check]
		if !ok {
			tracelog.ErrorLogger.Fatalf("Check %s is not available.", check)
		}
		checkTypes = append(checkTypes, checkType)
	}
	return checkTypes
}

func checkArgs(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		availableCheckCommands := make([]string, 0, len(availableChecks))
		for cmdName := range availableChecks {
			availableCheckCommands = append(availableCheckCommands, cmdName)
		}
		return errors.New("at least one of the following checks should be specified: " +
			strings.Join(availableCheckCommands, ", "))
	}
	for _, arg := range args {
		if _, ok := availableChecks[arg]; !ok {
			return fmt.Errorf("invalid check specified: %s", arg)
		}
	}
	return nil
}

func init() {
	cmd.AddCommand(walVerifyCmd)
	walVerifyCmd.Flags().BoolVar(&useJSONOutput, useJSONOutputFlag, false, useJSONOutputDescription)
}
