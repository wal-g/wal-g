package greenplum

import (
	"archive/tar"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/spf13/viper"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type GpTarBallComposerMaker struct {
	relStorageMap AoRelFileStorageMap
	bundleFiles   internal.BundleFiles
	TarFileSets   internal.TarFileSets
	uploader      *internal.Uploader
	backupName    string
}

func NewGpTarBallComposerMaker(relStorageMap AoRelFileStorageMap, uploader *internal.Uploader, backupName string,
) (*GpTarBallComposerMaker, error) {
	return &GpTarBallComposerMaker{
		relStorageMap: relStorageMap,
		bundleFiles:   &internal.RegularBundleFiles{},
		TarFileSets:   internal.NewRegularTarFileSets(),
		uploader:      uploader,
		backupName:    backupName,
	}, nil
}

func (maker *GpTarBallComposerMaker) Make(bundle *postgres.Bundle) (internal.TarBallComposer, error) {
	// checksums verification is not supported in Greenplum (yet)
	// TODO: Add support for checksum verification
	filePackerOptions := postgres.NewTarBallFilePackerOptions(false, false)

	baseFiles, err := maker.loadBaseFiles(bundle.IncrementFromName)
	if err != nil {
		return nil, err
	}

	filePacker := postgres.NewTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, maker.bundleFiles, filePackerOptions)
	tracelog.DebugLogger.Printf("basefiles: %v\n", baseFiles)
	aoStorageUploader := NewAoStorageUploader(
		maker.uploader, baseFiles, bundle.Crypter, maker.bundleFiles, bundle.IncrementFromName != "")

	return NewGpTarBallComposer(
		bundle.TarBallQueue,
		bundle.Crypter,
		maker.relStorageMap,
		maker.bundleFiles,
		filePacker,
		aoStorageUploader,
		maker.TarFileSets,
		maker.uploader,
		maker.backupName,
	)
}

func (maker *GpTarBallComposerMaker) loadBaseFiles(incrementFromName string) (BackupAOFiles, error) {
	var base SegBackup
	// In case of delta backup, use the provided backup name as the base. Otherwise, use the latest backup.
	if incrementFromName != "" {
		base = NewSegBackup(maker.uploader.UploadingFolder, incrementFromName)
	} else {
		backupName, err := internal.GetLatestBackupName(maker.uploader.UploadingFolder)
		if err != nil {
			if _, ok := err.(internal.NoBackupsFoundError); ok {
				tracelog.InfoLogger.Println("Couldn't find previous backup, leaving the base files empty.")
				return BackupAOFiles{}, nil
			}

			return nil, err
		}
		base = NewSegBackup(maker.uploader.UploadingFolder, backupName)
	}

	baseFilesMetadata, err := base.LoadAoFilesMetadata()
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); !ok {
			return nil, fmt.Errorf("failed to fetch AO files metadata for backup %s: %w", base.Backup.Name, err)
		}

		tracelog.WarningLogger.Printf(
			"AO files metadata was not found for backup %s, leaving the base files empty.", base.Backup.Name)
		return BackupAOFiles{}, nil
	}

	return baseFilesMetadata.Files, nil
}

type GpTarBallComposer struct {
	backupName    string
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *postgres.TarBallFilePackerImpl
	crypter       crypto.Crypter

	addFileQueue     chan *internal.ComposeFileInfo
	addFileWaitGroup sync.WaitGroup

	uploader *internal.Uploader

	files            internal.BundleFiles
	tarFileSets      internal.TarFileSets
	tarFileSetsMutex sync.Mutex

	errorGroup *errgroup.Group
	ctx        context.Context

	relStorageMap      AoRelFileStorageMap
	aoStorageUploader  *AoStorageUploader
	aoSegSizeThreshold int64
}

func NewGpTarBallComposer(
	tarBallQueue *internal.TarBallQueue, crypter crypto.Crypter, relStorageMap AoRelFileStorageMap,
	bundleFiles internal.BundleFiles, packer *postgres.TarBallFilePackerImpl, aoStorageUploader *AoStorageUploader,
	tarFileSets internal.TarFileSets, uploader *internal.Uploader, backupName string,
) (*GpTarBallComposer, error) {
	errorGroup, ctx := errgroup.WithContext(context.Background())

	composer := &GpTarBallComposer{
		backupName:         backupName,
		tarBallQueue:       tarBallQueue,
		tarFilePacker:      packer,
		crypter:            crypter,
		relStorageMap:      relStorageMap,
		files:              bundleFiles,
		aoStorageUploader:  aoStorageUploader,
		aoSegSizeThreshold: viper.GetInt64(internal.GPAoSegSizeThreshold),
		uploader:           uploader.Clone(),
		tarFileSets:        tarFileSets,
		errorGroup:         errorGroup,
		ctx:                ctx,
	}

	maxUploadDiskConcurrency, err := internal.GetMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	composer.addFileQueue = make(chan *internal.ComposeFileInfo, maxUploadDiskConcurrency)
	for i := 0; i < maxUploadDiskConcurrency; i++ {
		composer.addFileWaitGroup.Add(1)
		composer.errorGroup.Go(func() error {
			return composer.addFileWorker(composer.addFileQueue)
		})
	}
	return composer, nil
}

func (c *GpTarBallComposer) AddFile(info *internal.ComposeFileInfo) {
	select {
	case c.addFileQueue <- info:
		return
	case <-c.ctx.Done():
		tracelog.ErrorLogger.Printf("AddFile: not doing anything, err: %v", c.ctx.Err())
		return
	}
}

func (c *GpTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return c.errorGroup.Wait()
	}
	tarBall.SetUp(c.crypter)
	defer c.tarBallQueue.EnqueueBack(tarBall)
	c.tarFileSetsMutex.Lock()
	c.tarFileSets.AddFile(tarBall.Name(), fileInfoHeader.Name)
	c.tarFileSetsMutex.Unlock()
	c.files.AddFile(fileInfoHeader, info, false)
	return tarBall.TarWriter().WriteHeader(fileInfoHeader)
}

func (c *GpTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *GpTarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	close(c.addFileQueue)

	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}

	c.addFileWaitGroup.Wait()

	err = internal.UploadDto(c.uploader.UploadingFolder, c.aoStorageUploader.GetFiles(), getAOFilesMetadataPath(c.backupName))
	if err != nil {
		return nil, fmt.Errorf("failed to upload AO files metadata: %v", err)
	}
	return c.tarFileSets, nil
}

func (c *GpTarBallComposer) GetFiles() internal.BundleFiles {
	return c.files
}

func (c *GpTarBallComposer) addFileWorker(tasks <-chan *internal.ComposeFileInfo) error {
	for task := range tasks {
		err := c.addFile(task)
		if err != nil {
			tracelog.ErrorLogger.Printf(
				"Received an error while adding the file %s: %v", task.Path, err)
			return err
		}
	}
	c.addFileWaitGroup.Done()
	return nil
}

func (c *GpTarBallComposer) addFile(cfi *internal.ComposeFileInfo) error {
	// WAL-G uploads AO/AOCS relfiles to a different location
	isAo, meta, location := c.relStorageMap.getAOStorageMetadata(cfi.Path)
	if isAo && cfi.FileInfo.Size() >= c.aoSegSizeThreshold {
		tracelog.DebugLogger.Printf("%s is an AO/AOCS file, will process it through an AO storage manager",
			cfi.Path)
		return c.aoStorageUploader.AddFile(cfi, meta, location)
	}

	tracelog.DebugLogger.Printf("%s is not an AO/AOCS file, will process it through a regular tar file packer",
		cfi.Path)
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return err
	}
	tarBall.SetUp(c.crypter)
	c.tarFileSetsMutex.Lock()
	c.tarFileSets.AddFile(tarBall.Name(), cfi.Header.Name)
	c.tarFileSetsMutex.Unlock()
	c.errorGroup.Go(func() error {
		err := c.tarFilePacker.PackFileIntoTar(cfi, tarBall)
		if err != nil {
			return err
		}
		return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
	})
	return nil
}
