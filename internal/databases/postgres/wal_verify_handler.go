package postgres

import (
	"fmt"
	"io"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type WalVerifyCheckType int

const (
	WalVerifyIntegrityCheck = iota + 1
	WalVerifyTimelineCheck
)

func (checkType WalVerifyCheckType) String() string {
	return [...]string{"", "integrity", "timeline"}[checkType]
}

func (checkType WalVerifyCheckType) MarshalText() (text []byte, err error) {
	return utility.MarshalEnumToString(checkType)
}

type UnknownWalVerifyCheckError struct {
	error
}

func NewUnknownWalVerifyCheckError(checkType WalVerifyCheckType) UnknownWalVerifyCheckError {
	return UnknownWalVerifyCheckError{errors.Errorf("Unknown wal verify check: %s", checkType)}
}

func (err UnknownWalVerifyCheckError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type WalVerifyCheckStatus int

const (
	StatusOk WalVerifyCheckStatus = iota + 1
	StatusWarning
	StatusFailure
)

func (status WalVerifyCheckStatus) String() string {
	return [...]string{"", "OK", "WARNING", "FAILURE"}[status]
}

// MarshalText marshals the WalVerifyCheckStatus enum as a string
func (status WalVerifyCheckStatus) MarshalText() ([]byte, error) {
	return utility.MarshalEnumToString(status)
}

// WalVerifyCheckRunner performs the check of WAL storage
type WalVerifyCheckRunner interface {
	Type() WalVerifyCheckType
	Run() (WalVerifyCheckResult, error)
}

// WalVerifyCheckResult contains the result of some WalVerifyCheckRunner run
type WalVerifyCheckResult struct {
	Status  WalVerifyCheckStatus  `json:"status"`
	Details WalVerifyCheckDetails `json:"details"`
}

type WalVerifyCheckDetails interface {
	NewPlainTextReader() (io.Reader, error) // used in plaintext output
}

type NoCorrectBackupFoundError struct {
	error
}

func newNoCorrectBackupFoundError() NoCorrectBackupFoundError {
	return NoCorrectBackupFoundError{errors.Errorf("Could not find any correct backup in storage")}
}

func (err NoCorrectBackupFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// QueryCurrentWalSegment() gets start WAL segment from Postgres cluster
func QueryCurrentWalSegment() WalSegmentDescription {
	conn, err := Connect()
	tracelog.ErrorLogger.FatalfOnError("Failed to establish a connection to Postgres cluster %v", err)

	queryRunner, err := newPgQueryRunner(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize PgQueryRunner %v", err)

	currentSegmentNo, err := getCurrentWalSegmentNo(queryRunner)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current WAL segment number %v", err)

	currentTimeline, err := getCurrentTimeline(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current timeline %v", err)

	err = conn.Close()
	tracelog.WarningLogger.PrintOnError(err)

	// currentSegment is the current WAL segment of the cluster
	return WalSegmentDescription{Timeline: currentTimeline, Number: currentSegmentNo}
}

func BuildWalVerifyCheckRunner(
	checkType WalVerifyCheckType,
	rootFolder storage.Folder,
	walFolderFilenames []string,
	currentWalSegment WalSegmentDescription,
) (WalVerifyCheckRunner, error) {
	var checkRunner WalVerifyCheckRunner
	var err error
	switch checkType {
	case WalVerifyTimelineCheck:
		checkRunner, err = NewTimelineCheckRunner(walFolderFilenames, currentWalSegment)
	case WalVerifyIntegrityCheck:
		checkRunner, err = NewIntegrityCheckRunner(rootFolder, walFolderFilenames, currentWalSegment)
	default:
		return nil, NewUnknownWalVerifyCheckError(checkType)
	}
	if err != nil {
		return nil, err
	}

	return checkRunner, nil
}

// HandleWalVerify builds a check runner for each check type
// and writes the check results to the provided output writer
func HandleWalVerify(
	checkTypes []WalVerifyCheckType,
	rootFolder storage.Folder,
	currentWalSegment WalSegmentDescription,
	outputWriter WalVerifyOutputWriter,
) {
	checkResults := make(map[WalVerifyCheckType]WalVerifyCheckResult, len(checkTypes))

	// pre-fetch WAL folder filenames to reduce storage load
	walFolderFilenames, err := getFolderFilenames(rootFolder.GetSubFolder(utility.WalPath))
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch WAL folder filenames: %v", err)

	for _, checkType := range checkTypes {
		tracelog.InfoLogger.Printf("Building check runner: %s\n", checkType)
		runner, err := BuildWalVerifyCheckRunner(checkType, rootFolder, walFolderFilenames, currentWalSegment)
		tracelog.ErrorLogger.FatalfOnError(
			fmt.Sprintf("Failed to build check runner %s:", checkType), err)

		tracelog.InfoLogger.Printf("Running the check: %s\n", runner.Type().String())
		result, err := runner.Run()
		tracelog.ErrorLogger.FatalfOnError(
			fmt.Sprintf("Failed to run the check %s:", checkType), err)

		checkResults[runner.Type()] = result
	}

	err = outputWriter.Write(checkResults)
	tracelog.ErrorLogger.FatalOnError(err)
}

// get the current wal segment number of the cluster
func getCurrentWalSegmentNo(queryRunner *PgQueryRunner) (WalSegmentNo, error) {
	lsnStr, err := queryRunner.getCurrentLsn()
	if err != nil {
		return 0, err
	}
	lsn, err := pgx.ParseLSN(lsnStr)
	if err != nil {
		return 0, err
	}
	return newWalSegmentNo(lsn - 1), nil
}

// get the current timeline of the cluster
func getCurrentTimeline(conn *pgx.Conn) (uint32, error) {
	timeline, err := readTimeline(conn)
	if err != nil {
		return 0, err
	}
	return timeline, nil
}
