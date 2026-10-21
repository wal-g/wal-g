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
	ConsoleUsage      = "console"
	ConsoleShortDescr = "Run the debug console speaking the PostgreSQL wire protocol."
	ConsoleLongDescr  = "Serve a minimal PostgreSQL wire protocol so that regular clients (psql, " +
		"drivers) can connect to wal-g and inspect its state with SHOW queries: " +
		"SHOW VERSION and SHOW BACKUP_STATS (bytes and files of the latest backup " +
		"already uploaded to the storage, and the backup start)."

	defaultConsoleAddress = "127.0.0.1:15432"
)

var (
	// consoleCmd represents the console command
	consoleCmd = &cobra.Command{
		Use:   ConsoleUsage,
		Short: ConsoleShortDescr,
		Long:  ConsoleLongDescr,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			st, err := internal.ConfigureStorage(cmd.Context())
			tracelog.ErrorLogger.FatalOnError(err)

			address := viper.GetString(conf.ConsoleAddress)
			if address == "" {
				address = defaultConsoleAddress
			}

			listener, err := net.Listen("tcp", address)
			tracelog.ErrorLogger.FatalOnError(err)

			opts := pgdebug.Options{
				Version:             "wal-g " + walgVersion,
				BackupStatsProvider: backupStatsProvider(cmd.Context(), st.RootFolder()),
			}

			tracelog.InfoLogger.Printf("console listening on %s", listener.Addr())
			tracelog.ErrorLogger.FatalOnError(pgdebug.Serve(listener, opts))
		},
	}
)

// backupStatsProvider returns the provider powering SHOW BACKUP_STATS:
// it reports how many bytes and files of the latest backup are already
// uploaded to the storage, and where the backup started (LSN and time)
func backupStatsProvider(ctx context.Context, rootFolder storage.Folder) func() (pgdebug.BackupStats, error) {
	backupsFolder := rootFolder.GetSubFolder(utility.BaseBackupPath)
	return func() (pgdebug.BackupStats, error) {
		backup, err := internal.GetLatestBackup(ctx, backupsFolder)
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
}

func lsnToString(lsn *postgres.LSN) string {
	if lsn == nil {
		return ""
	}
	return lsn.String()
}

func init() {
	Cmd.AddCommand(consoleCmd)
}
