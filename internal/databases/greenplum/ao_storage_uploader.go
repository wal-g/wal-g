package greenplum

import (
	"io"
	"path"
	"sync"

	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/walparser"
)

type AoStorageUploader struct {
	uploader      internal.Uploader
	baseAoFiles   BackupAOFiles
	meta          *AOFilesMetadataDTO
	metaMutex     sync.Mutex
	crypter       crypto.Crypter
	bundleFiles   internal.BundleFiles
	isIncremental bool
}

func NewAoStorageUploader(uploader internal.Uploader, baseAoFiles BackupAOFiles,
	crypter crypto.Crypter, files internal.BundleFiles, isIncremental bool) *AoStorageUploader {
	// Separate uploader for AO/AOCS relfiles with disabled file size tracking since
	// WAL-G does not count them
	aoSegUploader := uploader.Clone()
	aoSegUploader.DisableSizeTracking()

	return &AoStorageUploader{
		uploader:      aoSegUploader,
		baseAoFiles:   baseAoFiles,
		meta:          NewAOFilesMetadataDTO(),
		crypter:       crypter,
		bundleFiles:   files,
		isIncremental: isIncremental,
	}
}

func (u *AoStorageUploader) AddFile(cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, location *walparser.BlockLocation) error {
	err := u.addFile(cfi, aoMeta, location)
	switch err.(type) {
	case internal.FileNotExistError:
		// File was deleted before opening.
		// We should ignore file here as if it did not exist.
		tracelog.WarningLogger.Println(err)
		return nil
	}

	return err
}

func (u *AoStorageUploader) addFile(cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, location *walparser.BlockLocation) error {
	remoteFile, ok := u.baseAoFiles[cfi.Header.Name]
	if !ok {
		tracelog.DebugLogger.Printf("%s: no base file in storage, will perform a regular upload", cfi.Header.Name)
		return u.regularAoUpload(cfi, aoMeta, location)
	}

	if aoMeta.modCount != remoteFile.ModCount {
		if !u.isIncremental || aoMeta.modCount == 0 {
			tracelog.DebugLogger.Printf("%s: isIncremental: %t, modCount: %d, will perform a regular upload",
				cfi.Header.Name, u.isIncremental, aoMeta.modCount)
			return u.regularAoUpload(cfi, aoMeta, location)
		}

		if aoMeta.eof == remoteFile.EOF {
			tracelog.WarningLogger.Printf(
				"%s: equal EOF %d, but local modcount %d is different from the remote %d, will perform a regular upload",
				cfi.Header.Name, aoMeta.eof, aoMeta.modCount, remoteFile.ModCount)
			return u.regularAoUpload(cfi, aoMeta, location)
		}

		tracelog.DebugLogger.Printf(
			"%s: EOF (local %d, remote %d), ModCount (local %d, remote %d), will perform an incremental upload",
			cfi.Header.Name, aoMeta.eof, remoteFile.EOF, aoMeta.modCount, remoteFile.ModCount)

		err := u.incrementalAoUpload(remoteFile.StoragePath, cfi, aoMeta, remoteFile.EOF)
		if err == nil {
			return nil
		}

		tracelog.WarningLogger.Printf("%s: incremental upload failed, will perform a regular upload: %v",
			cfi.Header.Name, err)
		return u.regularAoUpload(cfi, aoMeta, location)
	}

	if aoMeta.eof != remoteFile.EOF {
		tracelog.WarningLogger.Printf(
			"%s: equal modcount %d, but local EOF %d is different from the remote %d, will perform a regular upload",
			cfi.Header.Name, aoMeta.modCount, aoMeta.eof, remoteFile.EOF)
		return u.regularAoUpload(cfi, aoMeta, location)
	}

	tracelog.DebugLogger.Printf(
		"%s: ModCount %d, EOF %d matches the remote file %s, will skip this file",
		cfi.Header.Name, remoteFile.ModCount, remoteFile.EOF, remoteFile.StoragePath)
	return u.skipAoUpload(cfi, aoMeta, remoteFile.StoragePath)
}

func (u *AoStorageUploader) addAoFileMetadata(
	cfi *internal.ComposeFileInfo, storageKey string, aoMeta AoRelFileMetadata, isSkipped, isIncremented bool) {
	u.metaMutex.Lock()
	u.meta.addFile(cfi.Header.Name, storageKey, cfi.FileInfo.ModTime(), aoMeta, cfi.Header.Mode, isSkipped, isIncremented)
	u.metaMutex.Unlock()
}

func (u *AoStorageUploader) GetFiles() *AOFilesMetadataDTO {
	return u.meta
}

func (u *AoStorageUploader) skipAoUpload(cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, storageKey string) error {
	u.addAoFileMetadata(cfi, storageKey, aoMeta, true, false)
	u.bundleFiles.AddSkippedFile(cfi.Header, cfi.FileInfo)
	tracelog.DebugLogger.Printf("Skipping %s AO relfile (already exists in storage as %s)", cfi.Path, storageKey)
	return nil
}

func (u *AoStorageUploader) regularAoUpload(
	cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, location *walparser.BlockLocation) error {
	storageKey := makeAoFileStorageKey(aoMeta.relNameMd5, aoMeta.modCount, location)
	tracelog.DebugLogger.Printf("Uploading %s AO relfile to %s", cfi.Path, storageKey)
	fileReadCloser, err := internal.StartReadingFile(cfi.Header, cfi.FileInfo, cfi.Path)
	if err != nil {
		return err
	}

	defer fileReadCloser.Close()

	// do not compress AO/AOCS segment files since they are already compressed in most cases
	// TODO: lookup the compression details for each relation and compress it when compression is turned off
	var compressor compression.Compressor

	uploadContents := internal.CompressAndEncrypt(fileReadCloser, compressor, u.crypter, u.uploader.CompressedSizePtr())
	uploadPath := path.Join(AoStoragePath, storageKey)
	err = u.uploader.Upload(uploadPath, uploadContents)
	if err != nil {
		return err
	}

	u.addAoFileMetadata(cfi, storageKey, aoMeta, false, false)
	u.bundleFiles.AddFile(cfi.Header, cfi.FileInfo, false)
	return nil
}

func (u *AoStorageUploader) incrementalAoUpload(
	baseFileStorageKey string,
	cfi *internal.ComposeFileInfo, aoMeta AoRelFileMetadata, baseFileEOF int64) error {
	storageKey := makeDeltaAoFileStorageKey(baseFileStorageKey, aoMeta.modCount)
	tracelog.DebugLogger.Printf("Uploading %s AO relfile delta to %s", cfi.Path, storageKey)

	file, err := internal.StartReadingFile(cfi.Header, cfi.FileInfo, cfi.Path)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(file, "")

	incrementalReader, err := NewIncrementalPageReader(file, aoMeta.eof, baseFileEOF)
	if err != nil {
		return err
	}
	defer incrementalReader.Close()

	if err = u.upload(incrementalReader, storageKey); err != nil {
		return err
	}

	u.addAoFileMetadata(cfi, storageKey, aoMeta, false, true)
	u.bundleFiles.AddFile(cfi.Header, cfi.FileInfo, true)
	return nil
}

func (u *AoStorageUploader) upload(reader io.Reader, storageKey string) error {
	// do not compress AO/AOCS segment files since they are already compressed in most cases
	// TODO: lookup the compression details for each relation and compress it when compression is turned off
	var compressor compression.Compressor

	uploadContents := internal.CompressAndEncrypt(reader, compressor, u.crypter, u.uploader.CompressedSizePtr())
	uploadPath := path.Join(AoStoragePath, storageKey)
	return u.uploader.Upload(uploadPath, uploadContents)
}
