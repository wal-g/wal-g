package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type GenericMetaInteractor struct {
	GenericMetaFetcher
	GenericMetaSetter
}

func NewGenericMetaInteractor() GenericMetaInteractor {
	return GenericMetaInteractor{
		GenericMetaFetcher: NewGenericMetaFetcher(),
		GenericMetaSetter:  NewGenericMetaSetter(),
	}
}

func (mi GenericMetaInteractor) Fetch(ctx context.Context, name string, folder storage.Folder) (internal.GenericMetadata, error) {
	meta, err := mi.GenericMetaFetcher.Fetch(ctx, name, folder)
	if err != nil {
		return meta, err
	}
	doc, v2, err := readMarkSentinel(ctx, name, folder)
	if err != nil || !v2 {
		return meta, err
	}
	permanent, known := doc.Bool("IsPermanent")
	if !known {
		return meta, fmt.Errorf("unknown V2 protection for %s", name)
	}
	meta.NeedsPermanentReconciliation = meta.IsPermanent != permanent
	meta.IsPermanent = meta.IsPermanent || permanent
	return meta, nil
}

func (mi GenericMetaInteractor) FetchFromStorage(ctx context.Context, name string, folder storage.Folder,
	_ string) (internal.GenericMetadata, error) {
	return mi.Fetch(ctx, name, folder)
}

func readMarkSentinel(ctx context.Context, name string, folder storage.Folder) (internal.DeletionMetadata, bool, error) {
	doc, err := internal.ReadDeletionMetadata(ctx, folder, name+utility.SentinelSuffix, nil)
	if err != nil {
		return doc, false, err
	}
	if doc.Problem != nil {
		var missing storage.ObjectNotFoundError
		if stderrors.As(doc.Problem, &missing) {
			return doc, false, nil
		}
		return doc, false, doc.Problem
	}
	if raw, exists := doc.Fields["Version"]; exists {
		var version int
		if err := json.Unmarshal(raw, &version); err != nil {
			return doc, false, err
		}
		return doc, version == 2, nil
	}
	return doc, false, nil
}

type GenericMetaFetcher struct{}

func NewGenericMetaFetcher() GenericMetaFetcher {
	return GenericMetaFetcher{}
}

func (mf GenericMetaFetcher) Fetch(ctx context.Context, backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	backup, err := NewBackup(backupFolder, backupName)
	if err != nil {
		return internal.GenericMetadata{}, err
	}
	meta, err := backup.FetchMeta(ctx)
	if err != nil {
		return internal.GenericMetadata{}, err
	}

	return internal.GenericMetadata{
		BackupName:       backupName,
		UncompressedSize: meta.UncompressedSize,
		CompressedSize:   meta.CompressedSize,
		Hostname:         meta.Hostname,
		StartTime:        meta.StartTime,
		FinishTime:       meta.FinishTime,
		IsPermanent:      meta.IsPermanent,
		IncrementDetails: NewIncrementDetailsFetcher(ctx, backup),
		UserData:         meta.UserData,
	}, nil
}

// TODO rewrite multistorage pg operations with this method and internal.GetPermanentBackups instead of postgres.GetPermanentBackupsAndWals
func (mf GenericMetaFetcher) FetchFromStorage(
	ctx context.Context, backupName string, backupFolder storage.Folder, storage string,
) (internal.GenericMetadata, error) {
	return mf.Fetch(ctx, backupName, backupFolder)
}

type GenericMetaSetter struct{}

func NewGenericMetaSetter() GenericMetaSetter {
	return GenericMetaSetter{}
}

func (ms GenericMetaSetter) SetUserData(ctx context.Context, backupName string, backupFolder storage.Folder, userData interface{}) error {
	modifier := func(dto ExtendedMetadataDto) ExtendedMetadataDto {
		dto.UserData = userData
		return dto
	}
	return modifyBackupMetadata(ctx, backupName, backupFolder, modifier, false)
}

func (ms GenericMetaSetter) SetIsPermanent(ctx context.Context, backupName string, backupFolder storage.Folder, isPermanent bool) error {
	modifier := func(dto ExtendedMetadataDto) ExtendedMetadataDto {
		dto.IsPermanent = isPermanent
		return dto
	}
	return modifyBackupMetadata(ctx, backupName, backupFolder, modifier, true)
}

func modifyBackupMetadata(ctx context.Context,
	backupName string, backupFolder storage.Folder, modifier func(ExtendedMetadataDto) ExtendedMetadataDto, reconcileProtection bool) error {
	backup, err := internal.NewBackup(backupFolder, backupName)
	if err != nil {
		return errors.Wrap(err, "failed to modify metadata")
	}
	var meta ExtendedMetadataDto
	err = backup.FetchMetadata(ctx, &meta)
	if err != nil {
		return errors.Wrap(err, "failed to fetch the existing backup metadata for modifying")
	}
	meta = modifier(meta)
	if reconcileProtection {
		if err := reconcileMarkSentinel(ctx, backupName, backupFolder, meta.IsPermanent); err != nil {
			return err
		}
	}
	err = backup.UploadMetadata(ctx, meta)
	if err != nil {
		return errors.Wrap(err, "failed to upload the modified metadata to the storage")
	}
	return nil
}

func reconcileMarkSentinel(ctx context.Context, name string, folder storage.Folder, permanent bool) error {
	doc, v2, err := readMarkSentinel(ctx, name, folder)
	if err != nil || !v2 {
		return err
	}
	doc.Fields["IsPermanent"], _ = json.Marshal(permanent)
	raw, err := json.Marshal(doc.Fields)
	if err != nil {
		return err
	}
	return folder.PutObject(ctx, name+utility.SentinelSuffix, bytes.NewReader(raw))
}

type IncrementDetailsFetcher struct {
	ctx    context.Context //nolint:containedctx // IncrementDetails.Fetch interface method carries no ctx
	backup Backup
}

func NewIncrementDetailsFetcher(ctx context.Context, backup Backup) *IncrementDetailsFetcher {
	return &IncrementDetailsFetcher{ctx: ctx, backup: backup}
}

func (idf *IncrementDetailsFetcher) Fetch() (bool, internal.IncrementDetails, error) {
	sentinel, err := idf.backup.GetSentinel(idf.ctx)
	if err != nil || !sentinel.IsIncremental() {
		return false, internal.IncrementDetails{}, err
	}

	return true, internal.IncrementDetails{
		IncrementFrom:     *sentinel.IncrementFrom,
		IncrementFullName: *sentinel.IncrementFullName,
		IncrementCount:    *sentinel.IncrementCount,
	}, nil
}
