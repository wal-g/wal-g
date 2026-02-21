package pg

import (
	"fmt"
	"os"
	"strings"

	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/logging"

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

	useSpecifiedTimelineFlag        = "timeline"
	useSpecifiedTimelineDescription = "Verify WAL for the specified timeline. Works only in conjunction with the \"--lsn\" flag."

	useSpecifiedLsnFlag        = "lsn"
	useSpecifiedLsnDescription = "Verify WAL for the specified lsn. Works only in conjunction with the \"--timeline\" flag."

	useSpecifiedBackupFlag        = "backup-name"
	useSpecifiedBackupDescription = "Verify WAL starting from the specified backup."

	checkIntegrityArg = "integrity"
	checkTimelineArg  = "timeline"
)

var (
	availableChecks = map[string]postgres.WalVerifyCheckType{
		checkIntegrityArg: postgres.WalVerifyIntegrityCheck,
		checkTimelineArg:  postgres.WalVerifyTimelineCheck,
	}
	// walVerifyCmd represents the walVerify command
	walVerifyCmd = &cobra.Command{
		Use:   WalVerifyUsage,
		Short: WalVerifyShortDescription,
		Long:  WalVerifyLongDescription,
		Args:  checkArgs,
		Run: func(cmd *cobra.Command, checks []string) {
			storage, err := internal.ConfigureStorage()
			logging.FatalOnError(err)
			outputType := postgres.WalVerifyTableOutput
			if useJSONOutput {
				outputType = postgres.WalVerifyJSONOutput
			}
			outputWriter := postgres.NewWalVerifyOutputWriter(outputType, os.Stdout)
			checkTypes := parseChecks(checks)

			walSegmentDescription := getWalSegmentDescription(cmd, lsnStr, timeline)
			backupSearchParams := getBackupSearchParams(cmd, backupNameStr)
			postgres.HandleWalVerify(checkTypes, storage.RootFolder(), walSegmentDescription, backupSearchParams, outputWriter)
		},
	}
	useJSONOutput bool
	timeline      uint32
	lsnStr        string
	backupNameStr string
)

func getWalSegmentDescription(cmd *cobra.Command, lsnStr string, timeline uint32) postgres.WalSegmentDescription {
	if !cmd.Flags().Changed(useSpecifiedLsnFlag) {
		return postgres.QueryCurrentWalSegment()
	}
	lsn, err := postgres.ParseLSN(lsnStr)
	logging.FatalOnError(err)
	return postgres.WalSegmentDescription{
		Timeline: timeline,
		Number:   postgres.NewWalSegmentNo(lsn - 1),
	}
}

func getBackupSearchParams(cmd *cobra.Command, backupName string) postgres.BackupSearchParams {
	if cmd.Flags().Changed(useSpecifiedBackupFlag) {
		return postgres.BackupSearchParams{
			FindEarliestBackup:  false,
			SpecifiedBackupName: &backupName,
		}
	}
	return postgres.BackupSearchParams{
		FindEarliestBackup:  true,
		SpecifiedBackupName: nil,
	}
}

func parseChecks(checks []string) []postgres.WalVerifyCheckType {
	// filter the possible duplicates
	uniqueChecks := make(map[string]bool)
	for _, check := range checks {
		uniqueChecks[check] = true
	}

	checkTypes := make([]postgres.WalVerifyCheckType, 0, len(checks))
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
	isTimelineSpecified := cmd.Flags().Changed(useSpecifiedTimelineFlag)
	isLsnSpecified := cmd.Flags().Changed(useSpecifiedLsnFlag)

	if isLsnSpecified && !isTimelineSpecified {
		return fmt.Errorf("\"--lsn\" flag works only in conjunction with the \"--timeline\" flag")
	}
	if !isLsnSpecified && isTimelineSpecified {
		return fmt.Errorf("\"--timeline\" flag works only in conjunction with the \"--lsn\" flag")
	}
	return nil
}

func init() {
	Cmd.AddCommand(walVerifyCmd)
	walVerifyCmd.Flags().BoolVar(&useJSONOutput, useJSONOutputFlag, false, useJSONOutputDescription)
	walVerifyCmd.Flags().Uint32Var(&timeline, useSpecifiedTimelineFlag, 0, useSpecifiedTimelineDescription)
	walVerifyCmd.Flags().StringVar(&lsnStr, useSpecifiedLsnFlag, "", useSpecifiedLsnDescription)
	walVerifyCmd.Flags().StringVar(&backupNameStr, useSpecifiedBackupFlag, "", useSpecifiedBackupDescription)
}
