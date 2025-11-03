package greenplum

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// SegBackupMergeHandler performs merge of incremental backups for a single Greenplum segment.
// It mirrors the logic of postgres.BackupMergeHandler but uses Greenplum tar composer so that AO/AOCS files
// are uploaded to object storage and ao_files_metadata.json is generated for the merged backup.
type SegBackupMergeHandler struct {
	targetDeltaBackupName string
	targetDeltaBackup     postgres.Backup
	baseBackup            postgres.Backup
	storageFolder         storage.Folder
	resultBackupName      string
	// When true, perform deletion of old backup chain and garbage archives after merge (segment-local)
	cleanupAfterMerge bool
}

func NewSegBackupMergeHandler(targetBackupName string,
	storageFolder storage.Folder,
	cleanupAfterMerge bool,
) (*SegBackupMergeHandler, error) {
	// Use the basebackups subdirectory for backup operations
	backupFolder := storageFolder.GetSubFolder(utility.BaseBackupPath)

	targetDeltaBackup, err := internal.NewBackup(backupFolder, targetBackupName)
	if err != nil {
		return nil, err
	}

	// Get base backup name from sentinel
	pgTargetBackup := postgres.ToPgBackup(targetDeltaBackup)
	sentinel, err := pgTargetBackup.GetSentinel()
	if err != nil {
		return nil, err
	}

	var baseBackupName string
	if sentinel.IsIncremental() && sentinel.IncrementFullName != nil {
		baseBackupName = *sentinel.IncrementFullName
	} else {
		return nil, fmt.Errorf("target backup %s is not an incremental backup", targetBackupName)
	}

	baseBackup, err := internal.NewBackup(backupFolder, baseBackupName)
	if err != nil {
		return nil, err
	}

	// Remove the entire _D_xxxxx suffix to get the base backup name
	parts := strings.Split(targetBackupName, "_D_")
	resultBackupName := parts[0]

	return &SegBackupMergeHandler{
		targetDeltaBackupName: targetBackupName,
		targetDeltaBackup:     postgres.ToPgBackup(targetDeltaBackup),
		baseBackup:            postgres.ToPgBackup(baseBackup),
		storageFolder:         storageFolder,
		resultBackupName:      resultBackupName,
		cleanupAfterMerge:     cleanupAfterMerge,
	}, nil
}

// create a complete base backup with deltas
func (bm *SegBackupMergeHandler) fetchBackup(targetDir string) {
	selector, err := internal.NewTargetBackupSelector("", bm.targetDeltaBackupName, postgres.NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)
	// Use Greenplum ExtractProvider which loads AO metadata and builds an incremental interpreter accordingly
	extractProv := ExtractProviderImpl{}
	pgFetcher := postgres.GetFetcherOld(targetDir, "", "", extractProv)
	internal.HandleBackupFetch(bm.storageFolder, selector, pgFetcher)
}

func (bm *SegBackupMergeHandler) sendMergedBackup(tempDir string) {
	sentinel, err := bm.targetDeltaBackup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("[seg-merge] Merge backup from %s to %s. Result name %s",
		bm.baseBackup.Name, bm.targetDeltaBackupName, bm.resultBackupName)
	tracelog.InfoLogger.Printf("[seg-merge] Target sentinel %v", sentinel)

	mainBackupSentinel, err := getMainBackupSentinel(&bm.targetDeltaBackup, &sentinel)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("[seg-merge] main backup sentinel", mainBackupSentinel)

	compressor, err := internal.ConfigureCompressor()
	tracelog.ErrorLogger.FatalOnError(err)

	uploader := internal.NewRegularUploader(compressor, bm.targetDeltaBackup.Folder)
	tracelog.InfoLogger.Printf("[seg-merge] Uploader storageFolder %v", uploader.Folder())

	// uploading backup to storage using Greenplum composer to handle AO/AOCS
	arFileSets, bundle := bm.uploadMergedBackupToStorage(tempDir, uploader)

	// upload metadata
	err = bm.uploadMetadata(arFileSets, bundle, uploader)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (bm *SegBackupMergeHandler) uploadMergedBackupToStorage(
	tempDir string,
	uploader *internal.RegularUploader,
) (*internal.TarFileSets, *postgres.Bundle) {
	crypter := internal.ConfigureCrypter()
	bundle := postgres.NewBundle(tempDir, crypter, "",
		nil, nil, false,
		viper.GetInt64(conf.TarSizeThresholdSetting))
	tracelog.InfoLogger.Println("[seg-merge] bundle ", bundle)

	err := bundle.StartQueue(internal.NewStorageTarBallMaker(bm.resultBackupName, uploader))
	tracelog.InfoLogger.FatalOnError(err)

	// Build AO/AOCS relfile storage map
	pgConn, err := postgres.Connect()
	tracelog.ErrorLogger.FatalOnError(err)
	defer func() { _ = pgConn.Close(context.TODO()) }()
	gpRunner, err := NewGpQueryRunner(pgConn)
	tracelog.ErrorLogger.FatalOnError(err)
	relStorageMap, err := NewAoRelFileStorageMap(gpRunner)
	tracelog.ErrorLogger.FatalOnError(err)

	// Use Greenplum tarball composer maker to ensure AO handling and ao_files_metadata.json upload
	maker, err := NewGpTarBallComposerMaker(relStorageMap, uploader, bm.resultBackupName)
	tracelog.ErrorLogger.FatalOnError(err)

	err = bundle.SetupComposer(maker)
	tracelog.ErrorLogger.FatalOnError(err)

	err = filepath.Walk(tempDir, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)

	tarFileSets, err := bundle.FinishTarComposer()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("[seg-merge] Finishing queue ...")
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("[seg-merge] Uploading pg_control ...")
	err = bundle.UploadPgControl(uploader.Compression().FileExtension())
	tracelog.ErrorLogger.FatalOnError(err)

	// Upload postgres backup_label and tablespace_map files from the base backup
	archiveWithLabelFiles, err := bm.uploadLabelFiles(uploader)
	tracelog.ErrorLogger.FatalOnError(err)

	labelFilesList := []string{postgres.TablespaceMapFilename, postgres.BackupLabelFilename}

	tracelog.ErrorLogger.FatalOnError(err)
	tarFileSets.AddFiles(archiveWithLabelFiles, labelFilesList)
	// Wait for all uploads to finish.
	tracelog.DebugLogger.Println("[seg-merge] Waiting for all uploads to finish")
	uploader.Finish()
	if uploader.Failed() {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", bm.resultBackupName)
	}

	return &tarFileSets, bundle
}

func getMainBackupSentinel(backup *postgres.Backup, sentinel *postgres.BackupSentinelDto) (postgres.BackupSentinelDto, error) {
	incrementFullName, err := postgres.NewBackup(backup.Folder, *sentinel.IncrementFullName)
	if err != nil {
		return postgres.BackupSentinelDto{}, err
	}
	return incrementFullName.GetSentinel()
}

// return archive name with label files
func (bm *SegBackupMergeHandler) uploadLabelFiles(uploader internal.Uploader) (string, error) {
	_, metadataDto, err := bm.baseBackup.GetSentinelAndFilesMetadata()
	if err != nil {
		return "", err
	}

	var targetArchiveFileName string
	for archiveName, filesNames := range metadataDto.TarFileSets {
		for _, fileName := range filesNames {
			if fileName == postgres.BackupLabelFilename {
				targetArchiveFileName = archiveName
			}
		}
	}
	tracelog.DebugLogger.Println("[seg-merge] Archive with label", targetArchiveFileName)

	folder := bm.baseBackup.Folder.GetSubFolder(bm.targetDeltaBackupName)
	archiveWithLabel, err := folder.ReadObject(internal.TarPartitionFolderName + targetArchiveFileName)
	if err != nil {
		return "", err
	}
	defer archiveWithLabel.Close()

	targetPath := filepath.Join(bm.resultBackupName, internal.TarPartitionFolderName, targetArchiveFileName)
	err = uploader.Upload(context.Background(), targetPath, archiveWithLabel)
	if err != nil {
		return "", err
	}

	return targetArchiveFileName, nil
}

func (bm *SegBackupMergeHandler) uploadMetadata(tarFilesSets *internal.TarFileSets,
	bundle *postgres.Bundle,
	uploader *internal.RegularUploader,
) error {
	err := bm.uploadFilesMetadata(tarFilesSets, bundle, uploader)
	if err != nil {
		return err
	}
	err = bm.uploadExtendedMetadata(uploader)
	if err != nil {
		return err
	}

	return bm.UploadSentinel(uploader, bundle)
}

func (bm *SegBackupMergeHandler) uploadExtendedMetadata(uploader *internal.RegularUploader) error {
	sentinel, err := bm.targetDeltaBackup.GetSentinel()
	if err != nil {
		return err
	}
	// Fetch existing extended metadata from the target backup and reuse its fields
	existingMeta, err := bm.targetDeltaBackup.FetchMeta()
	if err != nil {
		return err
	}
	meta := postgres.NewExtendedMetadataDto(existingMeta.IsPermanent, existingMeta.DataDir, existingMeta.StartTime, sentinel)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	path := storage.JoinPath(bm.resultBackupName, utility.MetadataFileName)
	return uploader.Upload(context.Background(), path, bytes.NewReader(dtoBody))
}

func (bm *SegBackupMergeHandler) uploadFilesMetadata(tarFilesSets *internal.TarFileSets, bundle *postgres.Bundle,
	uploader *internal.RegularUploader) error {
	// TODO add method for get only files meta
	_, filesMetadata, err := bm.targetDeltaBackup.GetSentinelAndFilesMetadata()
	if err != nil {
		return err
	}
	// Build BackupFileList from bundle files
	filesList := buildBackupFileList(bundle.GetFiles())
	filesMeta := postgres.NewFilesMetadataDto(filesList, *tarFilesSets)
	// Preserve database name mapping from the target backup
	filesMeta.DatabasesByNames = filesMetadata.DatabasesByNames

	dtoBody, err := json.Marshal(filesMeta)
	if err != nil {
		return err
	}
	return uploader.Upload(context.Background(), bm.resultBackupName+"/"+postgres.FilesMetadataName, bytes.NewReader(dtoBody))
}

func (bm *SegBackupMergeHandler) UploadSentinel(uploader *internal.RegularUploader, bundle *postgres.Bundle) error {
	// Build V2 sentinel from target backup's sentinel and metadata
	sentinel, err := bm.targetDeltaBackup.GetSentinel()
	if err != nil {
		return err
	}
	existingMeta, err := bm.targetDeltaBackup.FetchMeta()
	if err != nil {
		return err
	}
	sentinelV2 := postgres.NewBackupSentinelDtoV2(sentinel, existingMeta)
	sentinelV2.IncrementFromLSN = nil
	sentinelV2.IncrementFrom = nil
	sentinelV2.IncrementFullName = nil
	sentinelV2.IncrementCount = nil
	sentinelV2.DataCatalogSize = atomic.LoadInt64(bundle.DataCatalogSize)
	uploadedDataSize, err := uploader.UploadedDataSize()
	if err != nil {
		return err
	}
	sentinelV2.CompressedSize = uploadedDataSize
	sentinelV2.UncompressedSize = atomic.LoadInt64(bundle.TarBallQueue.AllTarballsSize)
	dtoBody, err := json.Marshal(sentinelV2)
	if err != nil {
		return err
	}
	path := bm.resultBackupName + utility.SentinelSuffix
	return uploader.Upload(context.Background(), path, bytes.NewReader(dtoBody))
}

func (bm *SegBackupMergeHandler) HandleBackupMerge() {
	// create work directory
	tempDir := createTempDirForBackupMerge()
	defer removeDirectory(tempDir)

	// create a complete base backup with deltas
	bm.fetchBackup(tempDir)

	// archive and send backup with AO support
	bm.sendMergedBackup(tempDir)

	// optional: delete unused backups on segment
	if bm.cleanupAfterMerge {
		// Note: segment-level chain cleanup is orchestrated by master cleanup handler; keep noop here.
		tracelog.InfoLogger.Printf("[seg-merge] cleanupAfterMerge flag set, but cleanup is handled at master level; skipping.")
	}
}

func removeDirectory(path string) {
	err := os.RemoveAll(path)
	tracelog.ErrorLogger.FatalOnError(err)
}

func createTempDirForBackupMerge() string {
	tempDir, err := os.MkdirTemp("", "backup-merge")
	tracelog.ErrorLogger.FatalOnError(err)
	return tempDir
}

// buildBackupFileList converts a sync.Map of files into a typed BackupFileList
func buildBackupFileList(p *sync.Map) internal.BackupFileList {
	result := make(internal.BackupFileList)
	p.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(internal.BackupFileDescription)
		result[key] = description
		return true
	})
	return result
}
