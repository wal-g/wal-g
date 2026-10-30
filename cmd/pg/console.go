package pg

import (
	"context"
	"net"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/pgdebug"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	withConsoleFlag        = "with-console"
	withConsoleDescription = "Start the debug console speaking the PostgreSQL wire protocol alongside " +
		"the command, so that its progress can be inspected with SHOW queries " +
		"(e.g. SHOW BACKUP_STATS) via: psql \"host=127.0.0.1 port=15432 sslmode=disable dbname=test\""

	defaultConsoleAddress = "127.0.0.1:15432"
)

// consoleEnabled reports whether the --with-console flag was passed
func consoleEnabled(cmd *cobra.Command) bool {
	enabled, err := cmd.Flags().GetBool(withConsoleFlag)
	return err == nil && enabled
}

// startConsoleServer starts the debug console server in the background,
// so that it serves while the long-running command (backup-push,
// backup-fetch, daemon) is working, and dies with the process
func startConsoleServer(ctx context.Context) {
	address := viper.GetString(conf.ConsoleAddress)
	if address == "" {
		address = defaultConsoleAddress
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to start the console: %v", err)
		return
	}

	var rootFolder storage.Folder
	opts := pgdebug.Options{
		Version: "wal-g " + walgVersion,
		BackupStatsProvider: func() (pgdebug.BackupStats, error) {
			if rootFolder == nil {
				st, err := internal.ConfigureStorage(ctx)
				if err != nil {
					return pgdebug.BackupStats{}, err
				}
				rootFolder = st.RootFolder()
			}
			return backupStats(ctx, rootFolder)
		},
	}

	tracelog.InfoLogger.Printf("console listening on %s", listener.Addr())
	go func() {
		_ = pgdebug.Serve(listener, opts)
	}()
}

// backupStats reports how many bytes and files of the latest backup are
// already uploaded to the storage, and where the backup started (LSN and time)
func backupStats(ctx context.Context, rootFolder storage.Folder) (pgdebug.BackupStats, error) {
	backup, err := internal.GetLatestBackup(ctx, rootFolder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return pgdebug.BackupStats{}, err
	}
	pgBackup := postgres.ToPgBackup(backup)

	sentinel, filesMetadata, err := pgBackup.GetSentinelAndFilesMetadata(ctx)
	if err != nil {
		return pgdebug.BackupStats{}, err
	}

	stats := pgdebug.BackupStats{
		Name:              backup.Name,
		StartLSN:          lsnToString(sentinel.BackupStartLSN),
		FilesUploaded:     int64(len(filesMetadata.Files)),
		BytesUploaded:     sentinel.CompressedSize,
		BytesUncompressed: sentinel.UncompressedSize,
	}

	// the backup start time is stored in the V2 sentinel fields
	var sentinelV2 postgres.BackupSentinelDtoV2
	if err := pgBackup.FetchSentinel(ctx, &sentinelV2); err == nil && !sentinelV2.StartTime.IsZero() {
		stats.StartTime = sentinelV2.StartTime.Format(time.RFC3339)
	}
	return stats, nil
}

func lsnToString(lsn *postgres.LSN) string {
	if lsn == nil {
		return ""
	}
	return lsn.String()
}
