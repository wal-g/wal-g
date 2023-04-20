package binary

import (
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

type ConcurrentUploader struct {
	uploader *internal.RegularUploader
	bundle   *internal.Bundle

	UncompressedSize int64
	CompressedSize   int64
}

func CreateConcurrentUploader(uploader *internal.RegularUploader, backupName, directory string) (*ConcurrentUploader, error) {
	crypter := internal.ConfigureCrypter()
	tarSizeThreshold := viper.GetInt64(internal.TarSizeThresholdSetting)
	bundle := internal.NewBundle(directory, crypter, tarSizeThreshold, map[string]utility.Empty{})

	tracelog.InfoLogger.Println("Starting a new tar bundle")
	tarBallMaker := internal.NewStorageTarBallMaker(backupName, uploader)
	err := bundle.StartQueue(tarBallMaker)
	if err != nil {
		return nil, err
	}

	tarBallComposerMaker := internal.NewRegularTarBallComposerMaker(&internal.RegularBundleFiles{}, internal.NewRegularTarFileSets())
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
