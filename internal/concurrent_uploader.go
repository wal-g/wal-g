package internal

import (
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/utility"
)

type ConcurrentUploader struct {
	uploader Uploader
	bundle   *Bundle

	UncompressedSize int64
	CompressedSize   int64
}

func CreateConcurrentUploader(
	uploader Uploader,
	backupName string,
	directories []string,
	skipFileNotExists bool,
) (*ConcurrentUploader, error) {
	crypter := ConfigureCrypter()
	tarSizeThreshold := viper.GetInt64(conf.TarSizeThresholdSetting)
	bundle := NewBundle(directories, crypter, tarSizeThreshold, map[string]utility.Empty{})

	tracelog.InfoLogger.Println("Starting a new tar bundle")
	tarBallMaker := NewStorageTarBallMaker(backupName, uploader)
	err := bundle.StartQueue(tarBallMaker)
	if err != nil {
		return nil, err
	}

	tarBallComposerMaker := NewRegularTarBallComposerMaker(&RegularBundleFiles{}, NewRegularTarFileSets(), skipFileNotExists)
	err = bundle.SetupComposer(tarBallComposerMaker)
	if err != nil {
		return nil, err
	}

	return &ConcurrentUploader{
		uploader: uploader,
		bundle:   bundle,
	}, nil
}

func (concurrentUploader *ConcurrentUploader) UploadBackupFiles(backupFiles []*BackupFileMeta) error {
	for _, backupFileMeta := range backupFiles {
		err := concurrentUploader.Upload(backupFileMeta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (concurrentUploader *ConcurrentUploader) Upload(backupFile *BackupFileMeta) error {
	return concurrentUploader.bundle.AddToBundle(backupFile.Path, backupFile, nil)
}

func (concurrentUploader *ConcurrentUploader) Finalize() error {
	tracelog.InfoLogger.Println("Packing ...")
	_, err := concurrentUploader.bundle.FinishComposing()
	if err != nil {
		return err
	}

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = concurrentUploader.bundle.FinishQueue()
	if err != nil {
		return err
	}

	concurrentUploader.UncompressedSize = *concurrentUploader.bundle.TarBallQueue.AllTarballsSize
	concurrentUploader.CompressedSize, err = concurrentUploader.uploader.UploadedDataSize()
	return err
}

func (concurrentUploader *ConcurrentUploader) UploadSentinel(sentinelDto interface{}, backupName string) error {
	return UploadSentinel(concurrentUploader.uploader, sentinelDto, backupName)
}
