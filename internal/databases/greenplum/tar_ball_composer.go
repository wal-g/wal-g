package greenplum

import (
	"archive/tar"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type TarBallComposerType int

const (
	RegularComposer TarBallComposerType = iota + 1
	DatabaseComposer
)

func NewGpTarBallComposerMaker(composerType TarBallComposerType, relStorageMap AoRelFileStorageMap,
	uploader internal.Uploader, backupName string,
) (postgres.TarBallComposerMaker, error) {
	switch composerType {
	case RegularComposer:
		return NewRegularTarBallComposerMaker(relStorageMap, uploader, backupName)
	case DatabaseComposer:
		return NewDirDatabaseTarBallComposerMaker(relStorageMap, uploader, backupName)
	default:
		return nil, errors.New("NewTarBallComposerMaker: Unknown TarBallComposerType")
	}
}

type ComposerConstructor func(tarBallQueue *internal.TarBallQueue, crypter crypto.Crypter,
	relStorageMap AoRelFileStorageMap, bundleFiles internal.BundleFiles, packer *postgres.TarBallFilePackerImpl,
	aoStorageUploader *AoStorageUploader, tarFileSets internal.TarFileSets, uploader internal.Uploader,
	backupName string) (internal.TarBallComposer, error)

type RegularTarBallComposerMaker struct {
	relStorageMap AoRelFileStorageMap
	bundleFiles   internal.BundleFiles
	TarFileSets   internal.TarFileSets
	uploader      internal.Uploader
	backupName    string
	Constructor   ComposerConstructor
}

func NewRegularTarBallComposerMaker(relStorageMap AoRelFileStorageMap, uploader internal.Uploader, backupName string,
) (*RegularTarBallComposerMaker, error) {
	return &RegularTarBallComposerMaker{
		relStorageMap: relStorageMap,
		bundleFiles:   &internal.RegularBundleFiles{},
		TarFileSets:   internal.NewRegularTarFileSets(),
		uploader:      uploader,
		backupName:    backupName,
		Constructor:   NewRegularTarBallComposer,
	}, nil
}

func (maker *RegularTarBallComposerMaker) Make(bundle *postgres.Bundle) (internal.TarBallComposer, error) {
	// checksums verification is not supported in Greenplum (yet)
	// TODO: Add support for checksum verification
	filePackerOptions := postgres.NewTarBallFilePackerOptions(false, false)

	baseFiles, err := maker.loadBaseFiles(bundle.IncrementFromName)
	if err != nil {
		return nil, err
	}

	filePacker := postgres.NewTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, maker.bundleFiles, filePackerOptions)
	deduplicationAgeLimit, err := internal.GetDurationSetting(internal.GPAoDeduplicationAgeLimit)
	if err != nil {
		return nil, err
	}

	newAoSegFilesID := strconv.FormatInt(time.Now().UnixNano(), 10)
	aoStorageUploader := NewAoStorageUploader(
		maker.uploader, baseFiles, bundle.Crypter, maker.bundleFiles, bundle.IncrementFromName != "", deduplicationAgeLimit, newAoSegFilesID)

	return maker.Constructor(
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

func (maker *RegularTarBallComposerMaker) loadBaseFiles(incrementFromName string) (BackupAOFiles, error) {
	var base SegBackup
	// In case of delta backup, use the provided backup name as the base. Otherwise, use the latest backup.
	if incrementFromName != "" {
		base = NewSegBackup(maker.uploader.Folder(), incrementFromName)
	} else {
		backupName, err := internal.GetLatestBackupName(maker.uploader.Folder())
		if err != nil {
			if _, ok := err.(internal.NoBackupsFoundError); ok {
				tracelog.InfoLogger.Println("Couldn't find previous backup, leaving the base files empty.")
				return BackupAOFiles{}, nil
			}

			return nil, err
		}
		base = NewSegBackup(maker.uploader.Folder(), backupName)
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

type RegularTarBallComposer struct {
	backupName    string
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *postgres.TarBallFilePackerImpl
	crypter       crypto.Crypter

	addFileQueue     chan *internal.ComposeFileInfo
	addFileWaitGroup sync.WaitGroup

	uploader internal.Uploader

	files            internal.BundleFiles
	tarFileSets      internal.TarFileSets
	tarFileSetsMutex sync.Mutex

	errorGroup *errgroup.Group
	ctx        context.Context

	relStorageMap      AoRelFileStorageMap
	aoStorageUploader  *AoStorageUploader
	aoSegSizeThreshold int64
}

func NewRegularTarBallComposer(
	tarBallQueue *internal.TarBallQueue, crypter crypto.Crypter, relStorageMap AoRelFileStorageMap,
	bundleFiles internal.BundleFiles, packer *postgres.TarBallFilePackerImpl, aoStorageUploader *AoStorageUploader,
	tarFileSets internal.TarFileSets, uploader internal.Uploader, backupName string,
) (internal.TarBallComposer, error) {
	errorGroup, ctx := errgroup.WithContext(context.Background())

	composer := &RegularTarBallComposer{
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

func (c *RegularTarBallComposer) AddFile(info *internal.ComposeFileInfo) {
	select {
	case c.addFileQueue <- info:
		return
	case <-c.ctx.Done():
		tracelog.ErrorLogger.Printf("AddFile: not doing anything, err: %v", c.ctx.Err())
		return
	}
}

func (c *RegularTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
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

func (c *RegularTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RegularTarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	close(c.addFileQueue)

	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}

	c.addFileWaitGroup.Wait()

	err = internal.UploadDto(c.uploader.Folder(), c.aoStorageUploader.GetFiles(), getAOFilesMetadataPath(c.backupName))
	if err != nil {
		return nil, fmt.Errorf("failed to upload AO files metadata: %v", err)
	}
	return c.tarFileSets, nil
}

func (c *RegularTarBallComposer) GetFiles() internal.BundleFiles {
	return c.files
}

func (c *RegularTarBallComposer) addFileWorker(tasks <-chan *internal.ComposeFileInfo) error {
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

func (c *RegularTarBallComposer) addFile(cfi *internal.ComposeFileInfo) error {
	// WAL-G uploads AO/AOCS relfiles to a different location
	added, err := c.tryAddWithAoStorageManager(cfi)
	if err != nil {
		return err
	}
	if added {
		return nil
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

func (c *RegularTarBallComposer) tryAddWithAoStorageManager(cfi *internal.ComposeFileInfo) (bool, error) {
	isAo, meta, location := c.relStorageMap.getAOStorageMetadata(cfi.Path)
	if isAo && cfi.FileInfo.Size() >= c.aoSegSizeThreshold {
		tracelog.DebugLogger.Printf("%s is an AO/AOCS file, will process it through an AO storage manager",
			cfi.Path)
		return true, c.aoStorageUploader.AddFile(cfi, meta, location)
	}
	return false, nil
}
