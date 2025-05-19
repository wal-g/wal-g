package uploader

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/utility"
)

type ManagedUploader struct {
	uploader internal.Uploader
	bundle   *internal.Bundle

	UncompressedSize int64
	CompressedSize   int64
}

func CreateManagedUploader(uploader internal.Uploader, backupName string, directories []string) (*ManagedUploader, error) {
	crypter := internal.ConfigureCrypter()
	tarSizeThreshold := viper.GetInt64(conf.TarSizeThresholdSetting)
	bundle := internal.NewBundle(directories, crypter, tarSizeThreshold, map[string]utility.Empty{})

	tracelog.InfoLogger.Println("Starting a new tar bundle")
	tarBallMaker := internal.NewStorageTarBallMaker(backupName, uploader)
	err := bundle.StartQueue(tarBallMaker)
	if err != nil {
		return nil, err
	}

	err = bundle.SetupComposer(NewManagedTarBallComposerMaker())
	if err != nil {
		return nil, err
	}

	return &ManagedUploader{
		uploader: uploader,
		bundle:   bundle,
	}, nil
}

func (managedUploader *ManagedUploader) UploadBackupFiles(backupFiles []*internal.BackupFileMeta) error {

	for _, backupFileMeta := range backupFiles {
		err := managedUploader.Upload(backupFileMeta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (managedUploader *ManagedUploader) Upload(backupFile *internal.BackupFileMeta) error {
	return managedUploader.bundle.AddToBundle(backupFile.Path, backupFile, nil)
}

func (managedUploader *ManagedUploader) Finalize() error {
	tracelog.InfoLogger.Println("Packing ...")
	_, err := managedUploader.bundle.FinishComposing()
	if err != nil {
		return err
	}

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = managedUploader.bundle.FinishQueue()
	if err != nil {
		return err
	}

	managedUploader.UncompressedSize = *managedUploader.bundle.TarBallQueue.AllTarballsSize
	managedUploader.CompressedSize, err = managedUploader.uploader.UploadedDataSize()
	return err
}

func (managedUploader *ManagedUploader) UploadSentinel(sentinelDto interface{}, backupName string) error {
	return internal.UploadSentinel(managedUploader.uploader, sentinelDto, backupName)
}

func (managedUploader *ManagedUploader) GetFilesSet() (map[string][]string, error) {
	composer, ok := managedUploader.bundle.TarBallComposer.(*ManagedTarBallComposer)
	if !ok {
		return nil, errors.New("TarBallComposer does not implement ManagedTarBallComposer")
	}
	return composer.GetTarFileSets(), nil
}

func (managedUploader *ManagedUploader) CloseTarFile() error {
	composer, ok := managedUploader.bundle.TarBallComposer.(*ManagedTarBallComposer)
	if !ok {
		return errors.New("TarBallComposer does not implement ManagedTarBallComposer")
	}

	return composer.FinishTarBall()
}