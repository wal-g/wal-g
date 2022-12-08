package greenplum

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type SegBackup struct {
	postgres.Backup

	AoFilesMetadataDto *AOFilesMetadataDTO
}

func NewSegBackup(baseBackupFolder storage.Folder, name string) SegBackup {
	return SegBackup{
		Backup: postgres.NewBackup(baseBackupFolder, name),
	}
}

func ToGpSegBackup(source postgres.Backup) (output SegBackup) {
	return SegBackup{
		Backup: source,
	}
}

func (backup *SegBackup) LoadAoFilesMetadata() (*AOFilesMetadataDTO, error) {
	if backup.AoFilesMetadataDto != nil {
		return backup.AoFilesMetadataDto, nil
	}

	var meta AOFilesMetadataDTO
	err := internal.FetchDto(backup.Folder, &meta, getAOFilesMetadataPath(backup.Name))
	if err != nil {
		return nil, err
	}

	backup.AoFilesMetadataDto = &meta
	return backup.AoFilesMetadataDto, nil
}
