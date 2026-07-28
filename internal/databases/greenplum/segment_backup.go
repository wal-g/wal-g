package greenplum

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type SegBackup struct {
	postgres.Backup

	AoFilesMetadataDto  *AOFilesMetadataDTO
	PaxFilesMetadataDto *pax.FilesMetadataDTO
}

func NewSegBackup(ctx context.Context, baseBackupFolder storage.Folder, name, storage string) (SegBackup, error) {
	pgBackup, err := postgres.NewBackupInStorage(ctx, baseBackupFolder, name, storage)
	if err != nil {
		return SegBackup{}, err
	}
	return SegBackup{
		Backup: pgBackup,
	}, nil
}

func ToGpSegBackup(source postgres.Backup) (output SegBackup) {
	return SegBackup{
		Backup: source,
	}
}

func (backup *SegBackup) LoadAoFilesMetadata(ctx context.Context) (*AOFilesMetadataDTO, error) {
	if backup.AoFilesMetadataDto != nil {
		return backup.AoFilesMetadataDto, nil
	}

	var meta AOFilesMetadataDTO
	err := internal.FetchDto(ctx, backup.Folder, &meta, getAOFilesMetadataPath(backup.Name))
	if err != nil {
		return nil, err
	}

	backup.AoFilesMetadataDto = &meta
	return backup.AoFilesMetadataDto, nil
}

func (backup *SegBackup) LoadPaxFilesMetadata(ctx context.Context) (*pax.FilesMetadataDTO, error) {
	if backup.PaxFilesMetadataDto != nil {
		return backup.PaxFilesMetadataDto, nil
	}

	var meta pax.FilesMetadataDTO
	err := internal.FetchDto(ctx, backup.Folder, &meta, pax.GetFilesMetadataPath(backup.Name))
	if err != nil {
		return nil, err
	}

	backup.PaxFilesMetadataDto = &meta
	return backup.PaxFilesMetadataDto, nil
}
