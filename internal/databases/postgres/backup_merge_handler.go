package postgres

import (
	"bytes"
	"encoding/json"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// BackupMergeHandler holds all arguments for merge delta backup
type BackupMergeHandler struct {
	targetDeltaBackupName string
	targetDeltaBackup     Backup
	baseBackup            Backup
	storageFolder         storage.Folder
	composerType          TarBallComposerType
	resultBackupName      string
}

func NewBackupMergeHandler(targetBackupName string,
	storageFolder storage.Folder,
	tarBallComposerType TarBallComposerType) (*BackupMergeHandler, error) {
	baseBackupPath := storageFolder.GetSubFolder(utility.BaseBackupPath)
	targetDeltaBackup := NewBackup(baseBackupPath, targetBackupName)
	deltaBackupSentinel, err := targetDeltaBackup.GetSentinel()
	if err != nil {
		return nil, err
	}
	baseBackupName := deltaBackupSentinel.IncrementFullName
	baseBackup := NewBackup(baseBackupPath, *baseBackupName)
	resultBackupName := strings.Split(targetBackupName, DeltaBackupDelimiter)[0]
	return &BackupMergeHandler{
		targetDeltaBackupName: targetBackupName,
		targetDeltaBackup:     targetDeltaBackup,
		baseBackup:            baseBackup,
		storageFolder:         storageFolder,
		composerType:          tarBallComposerType,
		resultBackupName:      resultBackupName,
	}, nil
}

// create a complete base backup with deltas
func (bm *BackupMergeHandler) fetchBackup(targetDir string) {
	selector, err := internal.NewTargetBackupSelector("", bm.targetDeltaBackupName, NewGenericMetaFetcher())
	tracelog.ErrorLogger.FatalOnError(err)
	extractProv := ExtractProviderImpl{}
	fetcher := GetPgFetcherOld(targetDir, "", "", extractProv)
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
		Uploader: uploader,
	}
	// uploading backup to storage
	tarFileSets, bundle := bm.uploadMergedBackupToStorage(tempDir, &workers)

	// upload metadata
	err = bm.uploadMetadata(tarFileSets, bundle, uploader)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (bm *BackupMergeHandler) uploadMergedBackupToStorage(tempDir string, workers *BackupWorkers) (*internal.TarFileSets, *Bundle) {
	uploader := workers.Uploader

	crypter := internal.ConfigureCrypter()
	bundle := NewBundle(tempDir, crypter, "",
		nil, nil, false,
		viper.GetInt64(internal.TarSizeThresholdSetting))
	tracelog.InfoLogger.Println("bundle ", bundle)

	err := bundle.StartQueue(internal.NewStorageTarBallMaker(bm.resultBackupName, uploader))
	tracelog.InfoLogger.FatalOnError(err)

	maker, err := NewTarBallComposerMaker(bm.composerType, workers.QueryRunner,
		workers.Uploader, bm.resultBackupName,
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
	archiveWithLabelFiles, err := bm.uploadLabelFiles(&uploader)
	tracelog.ErrorLogger.FatalOnError(err)

	labelFilesList := []string{TablespaceMapFilename, BackupLabelFilename}

	tracelog.ErrorLogger.FatalOnError(err)
	tarFileSets.AddFiles(archiveWithLabelFiles, labelFilesList)
	timelineChanged := bundle.checkTimelineChanged(workers.QueryRunner)

	// Wait for all uploads to finish.
	tracelog.DebugLogger.Println("Waiting for all uploads to finish")
	workers.Uploader.Finish()
	if workers.Uploader.Failed() {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", bm.resultBackupName)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}

	return &tarFileSets, bundle
}

func getMainBackupSentinel(backup *Backup, sentinel *BackupSentinelDto) (BackupSentinelDto, error) {
	incrementFullName := NewBackup(backup.Folder, *sentinel.IncrementFullName)
	return incrementFullName.GetSentinel()
}

// baseBackup base backup from which delta backups were collected
// return archive name with label files
func (bm *BackupMergeHandler) uploadLabelFiles(uploader *internal.Uploader) (string, error) {
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
	err = (*uploader).Upload(targetPath, archiveWithLabel)
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
	return uploader.Upload(path, bytes.NewReader(dtoBody))
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
	return (*uploader).Upload(bm.resultBackupName+"/"+FilesMetadataName, bytes.NewReader(dtoBody))
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
	return uploader.Upload(path, bytes.NewReader(dtoBody))
}

func (bm *BackupMergeHandler) HandleBackupMerge() {
	// create work directory
	tempDir := createTempDirForBackupMerge()
	defer removeDirectory(tempDir)

	// create a complete base backup with deltas
	bm.fetchBackup(tempDir)

	// archive and send backup
	bm.sendMergedBackup(tempDir)
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
