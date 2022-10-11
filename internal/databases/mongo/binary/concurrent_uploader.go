package binary

import (
	"os"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

type ConcurrentUploader struct {
	uploader *internal.Uploader
	bundle   *internal.Bundle
}

func CreateConcurrentUploader(uploader *internal.Uploader, backupName, directory string) (*ConcurrentUploader, error) {
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

func (concurrentUploader *ConcurrentUploader) Upload(path string, fileInfo os.FileInfo) error {
	return concurrentUploader.bundle.AddToBundle(path, fileInfo, nil)
}

func (concurrentUploader *ConcurrentUploader) Finalize() (int64, int64, error) {
	tracelog.InfoLogger.Println("Packing ...")
	_, err := concurrentUploader.bundle.FinishComposing()
	if err != nil {
		return 0, 0, err
	}

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = concurrentUploader.bundle.FinishQueue()
	if err != nil {
		return 0, 0, err
	}

	uncompressedSize := *concurrentUploader.bundle.TarBallQueue.AllTarballsSize
	compressedSize, err := concurrentUploader.uploader.UploadedDataSize()
	if err != nil {
		return 0, 0, err
	}

	return uncompressedSize, compressedSize, err
}
