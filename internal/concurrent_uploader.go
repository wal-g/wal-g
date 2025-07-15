package internal

import (
	"context"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

type ConcurrentUploader struct {
	uploader Uploader
	bundle   *Bundle

	UncompressedSize int64
	CompressedSize   int64
}

type CreateConcurrentUploaderArgs struct {
	Uploader             Uploader
	BackupName           string
	Directory            string
	SkipFileNotExists    bool
	TarBallComposerMaker TarBallComposerMaker
}

func CreateConcurrentUploader(
	args CreateConcurrentUploaderArgs,
) (*ConcurrentUploader, error) {
	crypter := ConfigureCrypter()
	tarSizeThreshold := viper.GetInt64(conf.TarSizeThresholdSetting)
	bundle := NewBundle(args.Directory, crypter, tarSizeThreshold, map[string]utility.Empty{})

	tracelog.InfoLogger.Println("Starting a new tar bundle")
	tarBallMaker := NewStorageTarBallMaker(args.BackupName, args.Uploader)
	err := bundle.StartQueue(tarBallMaker)
	if err != nil {
		return nil, err
	}

	if args.TarBallComposerMaker == nil {
		args.TarBallComposerMaker = NewRegularTarBallComposerMaker(&RegularBundleFiles{}, NewRegularTarFileSets(), args.SkipFileNotExists)
	}

	err = bundle.SetupComposer(args.TarBallComposerMaker)
	if err != nil {
		return nil, err
	}

	return &ConcurrentUploader{
		uploader: args.Uploader,
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

func (concurrentUploader *ConcurrentUploader) UploadExactFile(ctx context.Context, file ioextensions.NamedReader) error {
	return concurrentUploader.uploader.UploadExactFile(ctx, file)
}

func (concurrentUploader *ConcurrentUploader) Upload(backupFile *BackupFileMeta) error {
	return concurrentUploader.bundle.AddToBundle(backupFile.Path, backupFile, nil)
}

func (concurrentUploader *ConcurrentUploader) Finalize() (TarFileSets, error) {
	tracelog.InfoLogger.Println("Packing ...")
	tarFileSets, err := concurrentUploader.bundle.FinishComposing()
	if err != nil {
		return nil, err
	}

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = concurrentUploader.bundle.FinishQueue()
	if err != nil {
		return nil, err
	}

	concurrentUploader.UncompressedSize = *concurrentUploader.bundle.TarBallQueue.AllTarballsSize
	concurrentUploader.CompressedSize, err = concurrentUploader.uploader.UploadedDataSize()
	return tarFileSets, err
}

func (concurrentUploader *ConcurrentUploader) UploadSentinel(sentinelDto interface{}, backupName string) error {
	return UploadSentinel(concurrentUploader.uploader, sentinelDto, backupName)
}
