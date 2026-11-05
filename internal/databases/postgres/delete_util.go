package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type PermanentObject struct {
	Name        string
	StorageName string
}

func GetPermanentBackupsAndWals(ctx context.Context, folder storage.Folder) (map[PermanentObject]bool, map[PermanentObject]bool) {
	backups, wals, err := GetPermanentBackupsAndWalsChecked(ctx, folder)
	tracelog.ErrorLogger.FatalOnError(err)
	return backups, wals
}

func GetPermanentBackupsAndWalsChecked(ctx context.Context, folder storage.Folder,
) (map[PermanentObject]bool, map[PermanentObject]bool, error) {
	times, err := internal.GetBackups(ctx, folder.GetSubFolder(utility.BaseBackupPath))
	var empty internal.NoBackupsFoundError
	if err != nil && !errors.As(err, &empty) {
		return nil, nil, err
	}
	backups, wals := map[PermanentObject]bool{}, map[PermanentObject]bool{}
	for _, item := range times {
		selected, err := multistorage.UseSpecificStorage(ctx, item.StorageName, folder)
		if err != nil {
			return nil, nil, err
		}
		b, err := LoadDeletionBackup(ctx, selected, item.BackupName)
		if err != nil {
			return nil, nil, err
		}
		if !b.ProtectionKnown || b.Problem != nil {
			return nil, nil, fmt.Errorf("cannot establish protection of %s: %v", item.BackupName, b.Problem)
		}
		if !b.Permanent {
			continue
		}
		data := b.Metadata.(DeletionBackupMetadata)
		if !data.LogsKnown {
			return nil, nil, fmt.Errorf("unknown WAL interval for permanent backup %s", item.BackupName)
		}
		timeline, err := ParseTimelineFromBackupName(item.BackupName)
		if err != nil {
			return nil, nil, err
		}
		first, last := NewWalSegmentNo(data.Meta.StartLsn-1), NewWalSegmentNo(data.Meta.FinishLsn-1)
		for segment := first; segment <= last; segment++ {
			wals[PermanentObject{segment.GetFilename(timeline), item.StorageName}] = true
		}
		backups[PermanentObject{item.BackupName, item.StorageName}] = true
	}
	return backups, wals, nil
}

func isBackupReservationFolder(folderPath string) bool {
	// Filesystem listings below a segment prefix can include a leading slash.
	folderPath = strings.TrimLeft(folderPath, "/")
	return folderPath == backupReservationsPath || strings.HasPrefix(folderPath, backupReservationsPath+"/")
}

func IsPermanent(objectName, storageName string, permanentBackups, permanentWals map[PermanentObject]bool) bool {
	// Recursive listings of filesystem subfolders can prepend a slash to
	// relative names, including when deleting a Greenplum segment's backups.
	objectName = strings.TrimLeft(objectName, "/")
	if strings.HasPrefix(objectName, backupReservationsPath+"/") {
		return true
	}
	if strings.HasPrefix(objectName, utility.WalPath) && len(objectName) >= len(utility.WalPath)+24 {
		wal := PermanentObject{
			Name:        objectName[len(utility.WalPath) : len(utility.WalPath)+24],
			StorageName: storageName,
		}
		return permanentWals[wal]
	}
	if strings.HasPrefix(objectName, utility.BaseBackupPath) {
		backup := PermanentObject{
			Name:        utility.StripLeftmostBackupName(objectName[len(utility.BaseBackupPath):]),
			StorageName: storageName,
		}
		return permanentBackups[backup]
	}
	// should not reach here, default to false
	return false
}
