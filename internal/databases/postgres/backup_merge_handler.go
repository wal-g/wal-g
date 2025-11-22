package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// BackupMergeHandler holds all arguments for merge delta backup
type BackupMergeHandler struct {
	targetDeltaBackupName string
	targetDeltaBackup     Backup
	baseBackup            Backup
	storageFolder         storage.Folder
	composerType          TarBallComposerType
	resultBackupName      string
	// When true, perform deletion of old backup chain and garbage archives after merge
	cleanupAfterMerge bool
}

func NewBackupMergeHandler(targetBackupName string,
	storageFolder storage.Folder,
	tarBallComposerType TarBallComposerType,
	cleanupAfterMerge bool) (*BackupMergeHandler, error) {
	// Use the basebackups subdirectory for backup operations
	backupFolder := storageFolder.GetSubFolder(utility.BaseBackupPath)

	targetDeltaBackup, err := internal.NewBackup(backupFolder, targetBackupName)
	if err != nil {
		return nil, err
	}

	// Get base backup name from sentinel
	pgTargetBackup := ToPgBackup(targetDeltaBackup)
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

	return &BackupMergeHandler{
		targetDeltaBackupName: targetBackupName,
		targetDeltaBackup:     ToPgBackup(targetDeltaBackup),
		baseBackup:            ToPgBackup(baseBackup),
		storageFolder:         storageFolder,
		composerType:          tarBallComposerType,
		resultBackupName:      resultBackupName,
		cleanupAfterMerge:     cleanupAfterMerge,
	}, nil
}

// create a complete base backup with deltas
func (bm *BackupMergeHandler) fetchBackup(targetDir string) {
	selector, err := internal.NewTargetBackupSelector("", bm.targetDeltaBackupName, NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)
	extractProv := ExtractProviderImpl{}
	fetcher := GetFetcherOld(targetDir, "", "", extractProv)
	internal.HandleBackupFetch(bm.storageFolder, selector, fetcher)
}

func (bm *BackupMergeHandler) sendMergedBackup(tempDir string) {
	sentinel, err := bm.targetDeltaBackup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)
	//baseBackupName := sentinel.IncrementFullName
	tracelog.InfoLogger.Printf("Merge backup from %s to %s. Result name %s",
		bm.baseBackup.Name, bm.targetDeltaBackupName, bm.resultBackupName)
	tracelog.InfoLogger.Printf("Sentinel %v", sentinel)

	mainBackupSentinel, err := getMainBackupSentinel(&bm.targetDeltaBackup, &sentinel)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("main backup sentinel", mainBackupSentinel)

	compressor, err := internal.ConfigureCompressor()
	tracelog.ErrorLogger.FatalOnError(err)

	uploader := internal.NewRegularUploader(compressor, bm.targetDeltaBackup.Folder)
	tracelog.InfoLogger.Printf("Uploader storageFolder %v", uploader.Folder())

	workers := BackupWorkers{
		Bundle:      nil,
		QueryRunner: nil,
	}
	// uploading backup to storage
	tarFileSets, bundle := bm.uploadMergedBackupToStorage(tempDir, &workers, uploader)

	// upload metadata
	err = bm.uploadMetadata(tarFileSets, bundle, uploader)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (bm *BackupMergeHandler) uploadMergedBackupToStorage(
	tempDir string,
	workers *BackupWorkers,
	uploader *internal.RegularUploader,
) (*internal.TarFileSets, *Bundle) {
	crypter := internal.ConfigureCrypter()
	bundle := NewBundle(tempDir, crypter, "",
		nil, nil, false,
		viper.GetInt64(conf.TarSizeThresholdSetting))
	tracelog.InfoLogger.Println("bundle ", bundle)

	err := bundle.StartQueue(internal.NewStorageTarBallMaker(bm.resultBackupName, uploader))
	tracelog.InfoLogger.FatalOnError(err)

	maker, err := NewTarBallComposerMaker(bm.composerType, workers.QueryRunner,
		uploader, bm.resultBackupName,
		NewTarBallFilePackerOptions(false, false),
		false)
	tracelog.InfoLogger.FatalOnError(err)

	err = bundle.SetupComposer(maker)
	tracelog.InfoLogger.FatalOnError(err)

	err = filepath.Walk(tempDir, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)

	tarFileSets, err := bundle.FinishTarComposer()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Uploading pg_control ...")
	err = bundle.UploadPgControl(uploader.Compression().FileExtension())
	tracelog.ErrorLogger.FatalOnError(err)

	//Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	//labelFilesTarBallName, labelFilesList, finishLsn, err := bundle.uploadLabelFiles(workers.QueryRunner)
	archiveWithLabelFiles, err := bm.uploadLabelFiles(uploader)
	tracelog.ErrorLogger.FatalOnError(err)

	labelFilesList := []string{TablespaceMapFilename, BackupLabelFilename}

	tracelog.ErrorLogger.FatalOnError(err)
	tarFileSets.AddFiles(archiveWithLabelFiles, labelFilesList)
	timelineChanged := bundle.checkTimelineChanged(workers.QueryRunner)

	// Wait for all uploads to finish.
	tracelog.DebugLogger.Println("Waiting for all uploads to finish")
	uploader.Finish()
	if uploader.Failed() {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", bm.resultBackupName)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}

	return &tarFileSets, bundle
}

func getMainBackupSentinel(backup *Backup, sentinel *BackupSentinelDto) (BackupSentinelDto, error) {
	incrementFullName, err := NewBackup(backup.Folder, *sentinel.IncrementFullName)
	if err != nil {
		return BackupSentinelDto{}, err
	}
	return incrementFullName.GetSentinel()
}

// baseBackup base backup from which delta backups were collected
// return archive name with label files
func (bm *BackupMergeHandler) uploadLabelFiles(uploader internal.Uploader) (string, error) {
	_, metadataDto, err := bm.baseBackup.GetSentinelAndFilesMetadata()
	if err != nil {
		return "", err
	}

	var targetArchiveFileName string
	for archiveName, filesNames := range metadataDto.TarFileSets {
		for _, fileName := range filesNames {
			if fileName == BackupLabelFilename {
				targetArchiveFileName = archiveName
			}
		}
	}
	tracelog.DebugLogger.Println("Archive with label", targetArchiveFileName)

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

func (bm *BackupMergeHandler) uploadMetadata(tarFilesSets *internal.TarFileSets,
	bundle *Bundle,
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

func (bm *BackupMergeHandler) uploadExtendedMetadata(uploader *internal.RegularUploader) error {
	sentinel, err := bm.targetDeltaBackup.GetSentinel()
	if err != nil {
		return err
	}
	sentinelV2, err := bm.targetDeltaBackup.getSentinelV2()
	if err != nil {
		return err
	}
	meta := NewExtendedMetadataDto(sentinelV2.IsPermanent, sentinelV2.DataDir, sentinelV2.StartTime, sentinel)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	path := storage.JoinPath(bm.resultBackupName, utility.MetadataFileName)
	return uploader.Upload(context.Background(), path, bytes.NewReader(dtoBody))
}

func (bm *BackupMergeHandler) uploadFilesMetadata(tarFilesSets *internal.TarFileSets, bundle *Bundle,
	uploader *internal.RegularUploader) error {
	// TODO add method for get only files meta
	_, filesMetadata, err := bm.targetDeltaBackup.GetSentinelAndFilesMetadata()
	if err != nil {
		return err
	}
	filesMeta := FilesMetadataDto{
		TarFileSets:      (*tarFilesSets).Get(),
		DatabasesByNames: filesMetadata.DatabasesByNames,
	}
	filesMeta.setFiles(bundle.GetFiles())

	dtoBody, err := json.Marshal(filesMeta)
	if err != nil {
		return err
	}
	return uploader.Upload(context.Background(), bm.resultBackupName+"/"+FilesMetadataName, bytes.NewReader(dtoBody))
}

func (bm *BackupMergeHandler) UploadSentinel(uploader *internal.RegularUploader, bundle *Bundle) error {
	sentinelV2, err := bm.targetDeltaBackup.getSentinelV2()
	if err != nil {
		return err
	}
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
	//path := storage.JoinPath(bm.resultBackupName, utility.MetadataFileName)
	path := bm.resultBackupName + utility.SentinelSuffix
	return uploader.Upload(context.Background(), path, bytes.NewReader(dtoBody))
}

// deleteUnusedBackup deletes old chain starting from base backup and cleans up outdated WAL archives.
func (bm *BackupMergeHandler) deleteUnusedBackup() {
	permanentBackups, permanentWals := GetPermanentBackupsAndWals(bm.storageFolder)

	deleteHandler, err := NewDeleteHandler(bm.storageFolder, permanentBackups, permanentWals, false)
	tracelog.ErrorLogger.FatalOnError(err)

	// Delete old base backup and all its dependent deltas
	targetSelector, err := internal.NewTargetBackupSelector("", bm.baseBackup.Name, NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler.HandleDeleteTarget(targetSelector, true, true)

	// Clean outdated WAL archives: use the newly created merged full backup as the deletion target
	// This avoids selecting an incremental backup as the oldest non-permanent target and failing with
	backupSelector, err := internal.NewTargetBackupSelector("", bm.resultBackupName, NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)
	resultTarget, err := deleteHandler.FindTargetBySelector(backupSelector)
	tracelog.ErrorLogger.FatalOnError(err)
	objSelector := func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), utility.WalPath) }
	folderFilter := func(string) bool { return true }
	tracelog.InfoLogger.Printf("Cleaning outdated WAL archives before merged backup: %s", bm.resultBackupName)
	err = deleteHandler.DeleteBeforeTargetWhere(resultTarget, true, objSelector, folderFilter)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (bm *BackupMergeHandler) HandleBackupMerge() {
	// create work directory
	tempDir := createTempDirForBackupMerge()
	defer removeDirectory(tempDir)

	// create a complete base backup with deltas
	bm.fetchBackup(tempDir)

	// archive and send backup
	bm.sendMergedBackup(tempDir)

	// delete unused backups
	if bm.cleanupAfterMerge {
		bm.deleteUnusedBackup()
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
