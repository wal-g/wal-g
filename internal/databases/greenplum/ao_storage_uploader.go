package greenplum

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/fsutil"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/utility"
	"github.com/zeebo/xxh3"
)

type AoStorageUploader struct {
	uploader      internal.Uploader
	baseAoFiles   BackupAOFiles
	meta          *AOFilesMetadataDTO
	metaMutex     sync.Mutex
	crypter       crypto.Crypter
	bundleFiles   internal.BundleFiles
	isIncremental bool
	// minimal age to use the AO/AOCS file deduplication
	deduplicationMinAge time.Time
	// unique identifier of the new AO/AOCS segments created by this uploader
	newAoSegFilesID string
}

func NewAoStorageUploader(uploader internal.Uploader, baseAoFiles BackupAOFiles,
	crypter crypto.Crypter, files internal.BundleFiles, isIncremental bool, deduplicationAgeLimit time.Duration,
	newAoSegFilesID string) *AoStorageUploader {
	// Separate uploader for AO/AOCS relfiles with disabled file size tracking since
	// WAL-G does not count them
	aoSegUploader := uploader.Clone()
	aoSegUploader.DisableSizeTracking()

	return &AoStorageUploader{
		uploader:            aoSegUploader,
		baseAoFiles:         baseAoFiles,
		meta:                NewAOFilesMetadataDTO(),
		crypter:             crypter,
		bundleFiles:         files,
		isIncremental:       isIncremental,
		deduplicationMinAge: time.Now().Add(-deduplicationAgeLimit),
		newAoSegFilesID:     newAoSegFilesID,
	}
}

func (u *AoStorageUploader) AddFile(ctx context.Context,
	cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, location *walparser.BlockLocation) error {
	err := u.addFile(ctx, cfi, aoMeta, location)
	switch err.(type) {
	case internal.FileNotExistError:
		// File was deleted before opening.
		// We should ignore file here as if it did not exist.
		tracelog.WarningLogger.Println(err)
		return nil
	}

	return err
}

func (u *AoStorageUploader) addFile(ctx context.Context,
	cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, location *walparser.BlockLocation) error {
	remoteFile, ok := u.baseAoFiles[cfi.Header.Name]
	if !ok {
		tracelog.DebugLogger.Printf("%s: no base file in storage, will perform a regular upload", cfi.Header.Name)
		return u.regularAoUpload(ctx, cfi, aoMeta, location)
	}

	if remoteFile.InitialUploadTS.Before(u.deduplicationMinAge) {
		tracelog.DebugLogger.Printf("%s: deduplication age limit passed (initial upload time: %s), will perform a regular upload",
			cfi.Header.Name, remoteFile.InitialUploadTS)
		return u.regularAoUpload(ctx, cfi, aoMeta, location)
	}

	if !u.isIncremental && remoteFile.IsIncremented {
		tracelog.DebugLogger.Printf("%s: backup isIncremental: %t, remote file isIncremented: %t, will perform a regular upload",
			cfi.Header.Name, u.isIncremental, remoteFile.IsIncremented)
		return u.regularAoUpload(ctx, cfi, aoMeta, location)
	}

	if aoMeta.modCount != remoteFile.ModCount {
		if !u.isIncremental || aoMeta.modCount == 0 {
			tracelog.DebugLogger.Printf("%s: isIncremental: %t, modCount: %d, will perform a regular upload",
				cfi.Header.Name, u.isIncremental, aoMeta.modCount)
			return u.regularAoUpload(ctx, cfi, aoMeta, location)
		}

		if aoMeta.eof <= remoteFile.EOF {
			tracelog.InfoLogger.Printf(
				"%s: less or equal EOF %d, but local modcount %d is different from the remote %d, will perform a regular upload",
				cfi.Header.Name, aoMeta.eof, aoMeta.modCount, remoteFile.ModCount)
			return u.regularAoUpload(ctx, cfi, aoMeta, location)
		}

		err, checksum, shouldIncrement := validateFileChecksum(ctx, cfi.Path, remoteFile.EOF, aoMeta.eof, remoteFile.Checksum)
		if err != nil || !shouldIncrement {
			tracelog.InfoLogger.Println("After checksum check will perform regular upload")
			return u.regularAoUpload(ctx, cfi, aoMeta, location)
		}

		tracelog.DebugLogger.Printf(
			"%s: EOF (local %d, remote %d), ModCount (local %d, remote %d), will perform an incremental upload",
			cfi.Header.Name, aoMeta.eof, remoteFile.EOF, aoMeta.modCount, remoteFile.ModCount)

		err = u.incrementalAoUpload(ctx, remoteFile.StoragePath, cfi, aoMeta, remoteFile.EOF, remoteFile.InitialUploadTS, checksum)
		if err == nil {
			return nil
		}

		tracelog.WarningLogger.Printf("%s: incremental upload failed, will perform a regular upload: %v",
			cfi.Header.Name, err)
		return u.regularAoUpload(ctx, cfi, aoMeta, location)
	}

	if aoMeta.eof != remoteFile.EOF {
		tracelog.WarningLogger.Printf(
			"%s: equal modcount %d, but local EOF %d is different from the remote %d, will perform a regular upload",
			cfi.Header.Name, aoMeta.modCount, aoMeta.eof, remoteFile.EOF)
		return u.regularAoUpload(ctx, cfi, aoMeta, location)
	}

	tracelog.DebugLogger.Printf(
		"%s: ModCount %d, EOF %d matches the remote file %s, will skip this file",
		cfi.Header.Name, remoteFile.ModCount, remoteFile.EOF, remoteFile.StoragePath)
	return u.skipAoUpload(cfi, aoMeta, remoteFile.StoragePath, remoteFile.InitialUploadTS, remoteFile.IsIncremented, remoteFile.Checksum)
}

func validateFileChecksum(ctx context.Context, path string, oldEof int64, curEof int64, previousChecksum string) (error, string, bool) {
	err, checksum := getCheckSum(ctx, path, oldEof)
	if err != nil {
		tracelog.InfoLogger.Printf("failed to count checksum for file %s with error: %v", path, err)
		return err, "", false
	}
	if checksum != previousChecksum || previousChecksum == "" {
		tracelog.InfoLogger.Printf("%s: remote file has different checksum from local. Previous: %s Local: %s", path, previousChecksum, checksum)
		return nil, "", false
	}

	err, newChecksum := getCheckSum(ctx, path, curEof)
	if err != nil {
		tracelog.InfoLogger.Printf("failed to count new checksum for file %s with error: %v", path, err)
		return err, "", false
	}
	return nil, newChecksum, true
}

func (u *AoStorageUploader) addAoFileMetadata(
	cfi *internal.ComposeFileInfo, storageKey string, aoMeta AoRelFileMetadata, isSkipped, isIncremented bool,
	initialUplTS time.Time, checksum string) {
	u.metaMutex.Lock()
	u.meta.addFile(cfi.Header.Name, storageKey, cfi.FileInfo.ModTime(),
		initialUplTS, aoMeta, cfi.Header.Mode, isSkipped, isIncremented, checksum)
	u.metaMutex.Unlock()
}

func (u *AoStorageUploader) GetFiles() *AOFilesMetadataDTO {
	return u.meta
}

func (u *AoStorageUploader) skipAoUpload(cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, storageKey string,
	initialUploadTS time.Time, isIncremented bool, checksum string) error {
	u.addAoFileMetadata(cfi, storageKey, aoMeta, true, isIncremented, initialUploadTS, checksum)
	u.bundleFiles.AddSkippedFile(cfi.Header, cfi.FileInfo)
	tracelog.DebugLogger.Printf("Skipping %s AO relfile (already exists in storage as %s)", cfi.Path, storageKey)
	return nil
}

func getCheckSum(ctx context.Context, filePath string, eof int64) (error, string) {
	file, err := fsutil.OpenReadOnlyMayBeDirectIO(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return internal.NewFileNotExistError(filePath), ""
		}
		return errors.Wrapf(err, "failed to open file '%s'\n", filePath), ""
	}
	diskLimitedFileReader := limiters.NewDiskLimitReader(ctx, file)

	hasher := xxh3.New128()
	if _, err = io.CopyN(hasher, diskLimitedFileReader, eof); err != nil {
		return err, ""
	}
	checksum := hex.EncodeToString(hasher.Sum(nil))
	return nil, checksum

}

func (u *AoStorageUploader) regularAoUpload(ctx context.Context,
	cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, location *walparser.BlockLocation) error {
	storageKey := makeAoFileStorageKey(aoMeta.relNameMd5, aoMeta.modCount, location, u.newAoSegFilesID)
	tracelog.DebugLogger.Printf("Uploading %s AO relfile to %s", cfi.Path, storageKey)
	fileReadCloser, err := internal.StartReadingFile(ctx, cfi.Header, cfi.FileInfo, cfi.Path)
	if err != nil {
		return err
	}

	defer fileReadCloser.Close()

	// Compute checksum while streaming the file to storage — zero extra I/O.
	// The hash is fed by a TeeReader that sits between the file reader and the
	// compressor/encryptor, so every byte is hashed exactly once as it is read.
	hasher := xxh3.New128()
	hashingReader := io.TeeReader(fileReadCloser, ioextensions.NewLimitedWriter(hasher, aoMeta.eof))

	// do not compress AO/AOCS segment files since they are already compressed in most cases
	// TODO: lookup the compression details for each relation and compress it when compression is turned off
	var compressor compression.Compressor

	uploadContents := internal.CompressAndEncrypt(hashingReader, compressor, u.crypter)
	uploadPath := path.Join(AoStoragePath, storageKey)
	err = u.uploader.Upload(ctx, uploadPath, uploadContents)
	if err != nil {
		return err
	}

	checksum := hex.EncodeToString(hasher.Sum(nil))
	u.addAoFileMetadata(cfi, storageKey, aoMeta, false, false, time.Now(), checksum)
	u.bundleFiles.AddFile(cfi.Header, cfi.FileInfo, false)
	return nil
}

func (u *AoStorageUploader) incrementalAoUpload(ctx context.Context,
	baseFileStorageKey string,
	cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, baseFileEOF int64, initialUploadTS time.Time, checksum string) error {
	storageKey := makeDeltaAoFileStorageKey(baseFileStorageKey, aoMeta.modCount)
	tracelog.DebugLogger.Printf("Uploading %s AO relfile delta to %s", cfi.Path, storageKey)

	file, err := internal.StartReadingFile(ctx, cfi.Header, cfi.FileInfo, cfi.Path)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(file, "")

	incrementalReader, err := NewIncrementalPageReader(ctx, file, aoMeta.eof, baseFileEOF)
	if err != nil {
		return err
	}
	defer incrementalReader.Close()

	if err = u.upload(ctx, incrementalReader, storageKey); err != nil {
		return err
	}

	u.addAoFileMetadata(cfi, storageKey, aoMeta, false, true, initialUploadTS, checksum)
	u.bundleFiles.AddFile(cfi.Header, cfi.FileInfo, true)
	return nil
}

func (u *AoStorageUploader) upload(ctx context.Context, reader io.Reader, storageKey string) error {
	// do not compress AO/AOCS segment files since they are already compressed in most cases
	// TODO: lookup the compression details for each relation and compress it when compression is turned off
	var compressor compression.Compressor

	uploadContents := internal.CompressAndEncrypt(reader, compressor, u.crypter)
	uploadPath := path.Join(AoStoragePath, storageKey)
	return u.uploader.Upload(ctx, uploadPath, uploadContents)
}
