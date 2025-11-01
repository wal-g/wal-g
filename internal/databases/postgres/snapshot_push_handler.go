package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/utility"
)

// SnapshotBackupArguments holds all arguments for snapshot backup
type SnapshotBackupArguments struct {
	Uploader        internal.Uploader
	pgDataDirectory string
	backupsFolder   string
	isPermanent     bool
	userData        interface{}
	snapshotCommand string
}

// SnapshotBackupHandler handles snapshot backup operations
type SnapshotBackupHandler struct {
	CurBackupInfo    CurBackupInfo
	Arguments        SnapshotBackupArguments
	QueryRunner      *PgQueryRunner
	PgInfo           BackupPgInfo
	startWalFileName string
	// Exact content from pg_stop_backup() - don't reconstruct, use what Postgres gives us
	backupLabel   string
	tablespaceMap string
}

// NewSnapshotBackupArguments creates a SnapshotBackupArguments object
func NewSnapshotBackupArguments(uploader internal.Uploader, pgDataDirectory string, backupsFolder string,
	isPermanent bool, userData interface{}, snapshotCommand string) SnapshotBackupArguments {
	return SnapshotBackupArguments{
		Uploader:        uploader,
		pgDataDirectory: pgDataDirectory,
		backupsFolder:   backupsFolder,
		isPermanent:     isPermanent,
		userData:        userData,
		snapshotCommand: snapshotCommand,
	}
}

// NewSnapshotBackupHandler creates a new SnapshotBackupHandler
func NewSnapshotBackupHandler(arguments SnapshotBackupArguments) (sbh *SnapshotBackupHandler, err error) {
	// Get PostgreSQL server info
	pgInfo, _, err := GetPgServerInfo(false)
	if err != nil {
		return nil, err
	}

	sbh = &SnapshotBackupHandler{
		Arguments: arguments,
		PgInfo:    pgInfo,
	}

	return sbh, nil
}

// HandleSnapshotPush handles the snapshot backup process
func (sbh *SnapshotBackupHandler) HandleSnapshotPush(ctx context.Context) {
	sbh.CurBackupInfo.StartTime = utility.TimeNowCrossPlatformUTC()

	if sbh.Arguments.pgDataDirectory == "" {
		tracelog.ErrorLogger.Fatal("PGDATA must be specified for snapshot backups")
	}

	// Verify data directory matches
	fromCli := sbh.Arguments.pgDataDirectory
	fromServer := sbh.PgInfo.PgDataDirectory
	if utility.AbsResolveSymlink(fromCli) != fromServer {
		tracelog.ErrorLogger.Fatalf("Data directory from command line '%s' is not the same as Postgres' one '%s'",
			fromCli, fromServer)
	}

	sbh.createAndPushSnapshotBackup(ctx)
}

func (sbh *SnapshotBackupHandler) createAndPushSnapshotBackup(ctx context.Context) {
	// Change to backups folder
	sbh.Arguments.Uploader.ChangeDirectory(sbh.Arguments.backupsFolder)
	tracelog.DebugLogger.Printf("Uploading folder: %s", sbh.Arguments.Uploader.Folder())

	// Step 1: Connect to postgres and start backup
	err := sbh.startBackup()
	tracelog.ErrorLogger.FatalOnError(err)

	// Step 2: Execute the snapshot command
	err = sbh.executeSnapshotCommand()
	if err != nil {
		// If snapshot command fails, we must stop the backup gracefully
		sbh.stopBackupOnError()
		tracelog.ErrorLogger.FatalOnError(err)
	}

	// Step 3: Stop backup and get finish LSN
	err = sbh.stopBackup()
	tracelog.ErrorLogger.FatalOnError(err)

	// Step 4: Upload sentinel metadata
	sbh.uploadSnapshotMetadata(ctx)

	tracelog.InfoLogger.Printf("Wrote snapshot backup with name %s", sbh.CurBackupInfo.Name)
}

func (sbh *SnapshotBackupHandler) startBackup() error {
	// Connect to postgres
	tracelog.DebugLogger.Println("Connecting to Postgres for snapshot backup.")
	conn, err := Connect()
	if err != nil {
		return err
	}

	sbh.QueryRunner, err = NewPgQueryRunner(conn)
	if err != nil {
		return errors.Wrap(err, "failed to build query runner")
	}

	// Call pg_start_backup
	tracelog.InfoLogger.Println("Calling pg_start_backup() for snapshot backup")
	backupName := utility.CeilTimeUpToMicroseconds(time.Now()).String()

	startBackupQuery, err := sbh.QueryRunner.BuildStartBackup()
	if err != nil {
		return errors.Wrap(err, "Building start backup query failed")
	}

	var walFileName string
	var lsnString string
	var inRecovery bool
	err = sbh.QueryRunner.Connection.QueryRow(context.TODO(), startBackupQuery, backupName).Scan(
		&walFileName, &lsnString, &inRecovery)
	if err != nil {
		return errors.Wrap(err, "pg_start_backup() failed")
	}

	if inRecovery {
		return errors.New("Cannot perform snapshot backup on a standby server")
	}

	// Parse start LSN
	sbh.CurBackupInfo.startLSN, err = ParseLSN(lsnString)
	if err != nil {
		return errors.Wrap(err, "failed to parse start LSN")
	}

	sbh.CurBackupInfo.Name = backupName

	// Store WAL file name for snapshot command
	if walFileName != "" {
		sbh.startWalFileName = walFileName
	} else {
		// On standby or when not available, calculate from LSN
		sbh.startWalFileName = NewWalSegmentNo(sbh.CurBackupInfo.startLSN - 1).GetFilename(sbh.PgInfo.Timeline)
	}

	tracelog.InfoLogger.Printf("Snapshot backup started: %s, LSN: %s, WAL: %s",
		backupName, lsnString, sbh.startWalFileName)

	return nil
}

func (sbh *SnapshotBackupHandler) executeSnapshotCommand() error {
	if sbh.Arguments.snapshotCommand == "" {
		return errors.New("Snapshot command is not configured (WALG_SNAPSHOT_COMMAND)")
	}

	tracelog.InfoLogger.Printf("Executing snapshot command: %s", sbh.Arguments.snapshotCommand)

	cmd := exec.Command("/bin/sh", "-c", sbh.Arguments.snapshotCommand)
	cmd.Env = append(os.Environ(),
		"WALG_SNAPSHOT_NAME="+sbh.CurBackupInfo.Name,
		"WALG_PG_DATA="+sbh.PgInfo.PgDataDirectory,
		"WALG_SNAPSHOT_START_LSN="+sbh.CurBackupInfo.startLSN.String(),
		"WALG_SNAPSHOT_START_WAL_FILE="+sbh.startWalFileName,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		tracelog.ErrorLogger.Printf("Snapshot command failed with output: %s", string(output))
		return errors.Wrapf(err, "snapshot command execution failed: %s", string(output))
	}

	tracelog.InfoLogger.Printf("Snapshot command completed successfully")
	tracelog.DebugLogger.Printf("Snapshot command output: %s", string(output))

	return nil
}

func (sbh *SnapshotBackupHandler) stopBackup() error {
	tracelog.InfoLogger.Println("Calling pg_stop_backup() for snapshot backup")

	stopBackupQuery, err := sbh.QueryRunner.BuildStopBackup()
	if err != nil {
		return errors.Wrap(err, "Building stop backup query failed")
	}

	var label, offsetMap, lsnStr string
	err = sbh.QueryRunner.Connection.QueryRow(context.TODO(), stopBackupQuery).Scan(&label, &offsetMap, &lsnStr)
	if err != nil {
		return errors.Wrap(err, "pg_stop_backup() failed")
	}

	// Store the exact content from PostgreSQL - don't reconstruct these files
	// The format may differ between PostgreSQL versions, so we must use what Postgres gives us
	sbh.backupLabel = label
	sbh.tablespaceMap = offsetMap

	// Parse finish LSN
	sbh.CurBackupInfo.endLSN, err = ParseLSN(lsnStr)
	if err != nil {
		return errors.Wrap(err, "failed to parse finish LSN")
	}

	tracelog.InfoLogger.Printf("Snapshot backup stopped, finish LSN: %s", lsnStr)
	tracelog.DebugLogger.Printf("Received backup_label content (%d bytes)", len(label))
	if offsetMap != "" {
		tracelog.DebugLogger.Printf("Received tablespace_map content (%d bytes)", len(offsetMap))
	}

	return nil
}

func (sbh *SnapshotBackupHandler) stopBackupOnError() {
	tracelog.WarningLogger.Println("Attempting to call pg_stop_backup() after snapshot command failure")

	if sbh.QueryRunner == nil {
		return
	}

	// Try to stop backup, but don't fail if this also errors
	_ = sbh.stopBackup()
}

func (sbh *SnapshotBackupHandler) uploadSnapshotMetadata(ctx context.Context) {
	// Create snapshot sentinel
	sentinelDto := sbh.createSnapshotSentinel()

	// Upload extended metadata
	meta := NewExtendedMetadataDto(sbh.Arguments.isPermanent, sbh.PgInfo.PgDataDirectory,
		sbh.CurBackupInfo.StartTime, sentinelDto)

	err := sbh.uploadExtendedMetadata(ctx, meta)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to upload metadata file for snapshot backup %s: %v",
			sbh.CurBackupInfo.Name, err)
	}

	// Upload sentinel
	err = internal.UploadSentinel(sbh.Arguments.Uploader, NewBackupSentinelDtoV2(sentinelDto, meta),
		sbh.CurBackupInfo.Name)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to upload sentinel file for snapshot backup %s: %v",
			sbh.CurBackupInfo.Name, err)
	}
}

func (sbh *SnapshotBackupHandler) createSnapshotSentinel() BackupSentinelDto {
	// Store exact content from PostgreSQL - never reconstruct these files
	var backupLabelPtr *string
	if sbh.backupLabel != "" {
		backupLabelPtr = &sbh.backupLabel
	}

	var tablespaceMapPtr *string
	if sbh.tablespaceMap != "" {
		tablespaceMapPtr = &sbh.tablespaceMap
	}

	sentinel := BackupSentinelDto{
		BackupStartLSN:   &sbh.CurBackupInfo.startLSN,
		BackupFinishLSN:  &sbh.CurBackupInfo.endLSN,
		PgVersion:        sbh.PgInfo.PgVersion,
		SystemIdentifier: sbh.PgInfo.systemIdentifier,
		UserData:         sbh.Arguments.userData,
		// Snapshot backups don't track file sizes or use compression
		UncompressedSize: 0,
		CompressedSize:   0,
		DataCatalogSize:  0,
		// Mark that this is a snapshot backup (no files metadata)
		FilesMetadataDisabled: true,
		// Snapshot backups are always full, not incremental
		IncrementFromLSN:  nil,
		IncrementFrom:     nil,
		IncrementFullName: nil,
		IncrementCount:    nil,
		// Store exact content from pg_stop_backup()
		BackupLabel:   backupLabelPtr,
		TablespaceMap: tablespaceMapPtr,
	}

	return sentinel
}

func (sbh *SnapshotBackupHandler) uploadExtendedMetadata(ctx context.Context, meta ExtendedMetadataDto) error {
	metaFile := utility.MetadataFileName
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return internal.NewSentinelMarshallingError(metaFile, err)
	}

	metaPath := sbh.CurBackupInfo.Name + "/" + metaFile
	tracelog.DebugLogger.Printf("Uploading metadata file (%s):\n%s", metaPath, dtoBody)

	return sbh.Arguments.Uploader.Upload(ctx, metaPath, bytes.NewReader(dtoBody))
}

// GetSnapshotCommand reads the snapshot command from configuration
func GetSnapshotCommand() (string, error) {
	snapshotCmd, ok := conf.GetSetting(conf.PgSnapshotCommand)
	if !ok || snapshotCmd == "" {
		return "", errors.New("WALG_SNAPSHOT_COMMAND is not configured")
	}
	return snapshotCmd, nil
}

// GetSnapshotDeleteCommand reads the snapshot delete command from configuration (optional)
func GetSnapshotDeleteCommand() (string, bool) {
	snapshotDeleteCmd, ok := conf.GetSetting(conf.PgSnapshotDeleteCommand)
	return snapshotDeleteCmd, ok
}
