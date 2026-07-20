package pax

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

// StorageUploader routes PAX files to the dedicated wal-g `paxfiles/` storage prefix
// and decides per file whether to skip (already in storage from a prior backup)
// or upload whole.
type StorageUploader struct {
	uploader            internal.Uploader
	baseFiles           BackupFiles
	meta                *FilesMetadataDTO
	metaMutex           sync.Mutex
	crypter             crypto.Crypter
	bundleFiles         internal.BundleFiles
	deduplicationMinAge time.Time
	newPaxFilesID       string
}

func NewStorageUploader(uploader internal.Uploader, baseFiles BackupFiles, crypter crypto.Crypter,
	files internal.BundleFiles, deduplicationAgeLimit time.Duration, newPaxFilesID string) *StorageUploader {
	// Separate uploader for PAX files with disabled file size tracking, matching the AO/AOCS handling path.
	paxFileUploader := uploader.Clone()
	paxFileUploader.DisableSizeTracking()

	return &StorageUploader{
		uploader:            paxFileUploader,
		baseFiles:           baseFiles,
		meta:                NewFilesMetadataDTO(),
		crypter:             crypter,
		bundleFiles:         files,
		deduplicationMinAge: time.Now().Add(-deduplicationAgeLimit),
		newPaxFilesID:       newPaxFilesID,
	}
}

func (u *StorageUploader) GetFiles() *FilesMetadataDTO {
	return u.meta
}

func (u *StorageUploader) AddFile(ctx context.Context,
	cfi *internal.ComposeFileInfo, meta RelFileMetadata, fileKey FileKey) error {
	err := u.addFile(ctx, cfi, meta, fileKey)
	if _, ok := err.(internal.FileNotExistError); ok {
		// File was deleted between walk and open. Treat as "did not exist".
		tracelog.WarningLogger.Println(err)
		return nil
	}
	return err
}

func (u *StorageUploader) addFile(ctx context.Context,
	cfi *internal.ComposeFileInfo, meta RelFileMetadata, fileKey FileKey) error {
	remoteFile, ok := u.baseFiles[cfi.Header.Name]
	if !ok {
		tracelog.DebugLogger.Printf("%s: no base PAX file in storage, will upload", cfi.Header.Name)
		return u.regularUpload(ctx, cfi, meta, fileKey)
	}

	if remoteFile.InitialUploadTS.Before(u.deduplicationMinAge) {
		tracelog.DebugLogger.Printf("%s: PAX dedup age limit passed (initial upload %s), will re-upload",
			cfi.Header.Name, remoteFile.InitialUploadTS)
		return u.regularUpload(ctx, cfi, meta, fileKey)
	}

	// if dedup check failed -> upload new file
	if remoteFile.RelNameMd5 != meta.RelNameMd5 || remoteFile.BlockID != meta.BlockID || remoteFile.Kind != meta.Kind {
		tracelog.DebugLogger.Printf(
			"%s: PAX identity mismatch (remote md5=%s blockid=%d kind=%s vs local md5=%s blockid=%d kind=%s), will re-upload",
			cfi.Header.Name,
			remoteFile.RelNameMd5, remoteFile.BlockID, remoteFile.Kind,
			meta.RelNameMd5, meta.BlockID, meta.Kind)
		return u.regularUpload(ctx, cfi, meta, fileKey)
	}

	tracelog.DebugLogger.Printf("%s: PAX file already in storage as %s, will skip",
		cfi.Header.Name, remoteFile.StoragePath)
	return u.skipUpload(cfi, meta, remoteFile.StoragePath, remoteFile.InitialUploadTS)
}

func (u *StorageUploader) skipUpload(cfi *internal.ComposeFileInfo, meta RelFileMetadata,
	storageKey string, initialUploadTS time.Time) error {
	u.addMetadata(cfi, storageKey, meta, true, initialUploadTS)
	u.bundleFiles.AddSkippedFile(cfi.Header, cfi.FileInfo)
	tracelog.DebugLogger.Printf("Skipping %s PAX file (already exists in storage as %s)", cfi.Path, storageKey)
	return nil
}

func (u *StorageUploader) regularUpload(ctx context.Context,
	cfi *internal.ComposeFileInfo, meta RelFileMetadata, fileKey FileKey) error {
	storageKey := MakeFileStorageKey(meta.RelNameMd5, fileKey, u.newPaxFilesID)
	tracelog.DebugLogger.Printf("Uploading %s PAX file to %s", cfi.Path, storageKey)

	fileReadCloser, err := internal.StartReadingFile(ctx, cfi.Header, cfi.FileInfo, cfi.Path)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(fileReadCloser, "Failed to close PAX file")

	// PAX/PORC files are already compressed internally; do not re-compress.
	var compressor compression.Compressor

	uploadContents := internal.CompressAndEncrypt(fileReadCloser, compressor, u.crypter)
	uploadPath := path.Join(StoragePath, storageKey)
	if err := u.uploader.Upload(ctx, uploadPath, uploadContents); err != nil {
		return err
	}

	u.addMetadata(cfi, storageKey, meta, false, time.Now())
	u.bundleFiles.AddFile(cfi.Header, cfi.FileInfo, false)
	return nil
}

func (u *StorageUploader) addMetadata(cfi *internal.ComposeFileInfo, storageKey string,
	meta RelFileMetadata, isSkipped bool, initialUplTS time.Time) {
	u.metaMutex.Lock()
	defer u.metaMutex.Unlock()
	u.meta.AddFile(cfi.Header.Name, storageKey, cfi.FileInfo.ModTime(), initialUplTS,
		meta, cfi.Header.Mode, isSkipped)
}
