// pgdebug-demo runs a PostgreSQL wire protocol debug server which
// reports wal-g state over SHOW queries, e.g.:
//
//	psql "host=127.0.0.1 port=15432 sslmode=disable dbname=test" \
//	    -c 'SHOW VERSION' -c 'SHOW BACKUP_STATS'
package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/pgdebug"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func main() {
	ctx := context.Background()

	config.InitConfig()
	if err := config.ConfigureLogging(); err != nil {
		log.Fatalf("failed to configure logging: %v", err)
	}

	multiStorage, err := internal.ConfigureMultiStorage(ctx, false)
	if err != nil {
		log.Fatalf("failed to configure storages: %v", err)
	}

	opts := pgdebug.Options{
		Version:             "wal-g debug 0.1",
		BackupStatsProvider: backupStatsProvider(ctx, multiStorage.RootFolder()),
	}

	l, err := net.Listen("tcp", "127.0.0.1:15432")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("debug server listening on", l.Addr())
	log.Fatal(pgdebug.Serve(l, opts))
}

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
			StartLSN:          lsnString(sentinel.BackupStartLSN),
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

func lsnString(lsn *postgres.LSN) string {
	if lsn == nil {
		return ""
	}
	return lsn.String()
}
