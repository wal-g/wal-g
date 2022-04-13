package postgres

import (
	"archive/tar"
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/wal-g/wal-g/internal/walparser"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"

	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type GpTarBallComposerMaker struct {
	relStorageMap AoRelFileStorageMap
	bundleFiles   BundleFiles
	TarFileSets   TarFileSets
	aoFiles       *AOFilesMetadataDTO
	uploader      *internal.Uploader
	baseFiles     map[string]struct{}
	backupName    string
}

func NewGpTarBallComposerMaker(relStorageMap AoRelFileStorageMap, uploader *internal.Uploader, backupName string,
) (*GpTarBallComposerMaker, error) {
	baseFiles, err := LoadStorageAoFiles(uploader.UploadingFolder)
	if err != nil {
		return nil, err
	}

	return &GpTarBallComposerMaker{
		relStorageMap: relStorageMap,
		bundleFiles:   &RegularBundleFiles{},
		TarFileSets:   NewRegularTarFileSets(),
		aoFiles:       NewAOFilesMetadataDTO(),
		uploader:      uploader,
		baseFiles:     baseFiles,
		backupName:    backupName,
	}, nil
}

func (maker *GpTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	// checksums verification is not supported in Greenplum (yet)
	// TODO: Add support for checksum verification
	filePackerOptions := TarBallFilePackerOptions{
		verifyPageChecksums:   false,
		storeAllCorruptBlocks: false,
	}
	filePacker := newTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, maker.bundleFiles, filePackerOptions)
	return NewGpTarBallComposer(
		bundle.TarBallQueue,
		bundle.Crypter,
		maker.relStorageMap,
		maker.bundleFiles,
		filePacker,
		maker.aoFiles,
		maker.baseFiles,
		maker.TarFileSets,
		maker.uploader,
		maker.backupName,
	)
}

type GpTarBallComposer struct {
	backupName    string
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter

	addFileQueue     chan *ComposeFileInfo
	addFileWaitGroup sync.WaitGroup

	relStorageMap AoRelFileStorageMap
	aoFiles       *AOFilesMetadataDTO
	aoFilesMutex  sync.Mutex
	baseAoFiles   map[string]struct{}

	uploader *internal.Uploader

	files            BundleFiles
	tarFileSets      TarFileSets
	tarFileSetsMutex sync.Mutex

	errorGroup *errgroup.Group
	ctx        context.Context
}

func NewGpTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	crypter crypto.Crypter, relStorageMap AoRelFileStorageMap, bundleFiles BundleFiles, packer *TarBallFilePacker,
	aoFiles *AOFilesMetadataDTO, baseAoFiles map[string]struct{}, tarFileSets TarFileSets, uploader *internal.Uploader,
	backupName string,
) (*GpTarBallComposer, error) {
	errorGroup, ctx := errgroup.WithContext(context.Background())

	composer := &GpTarBallComposer{
		backupName:    backupName,
		tarBallQueue:  tarBallQueue,
		tarFilePacker: packer,
		crypter:       crypter,
		relStorageMap: relStorageMap,
		files:         bundleFiles,
		aoFiles:       aoFiles,
		baseAoFiles:   baseAoFiles,
		uploader:      uploader.Clone(),
		tarFileSets:   tarFileSets,
		errorGroup:    errorGroup,
		ctx:           ctx,
	}

	maxUploadDiskConcurrency, err := internal.GetMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	composer.addFileQueue = make(chan *ComposeFileInfo, maxUploadDiskConcurrency)
	for i := 0; i < maxUploadDiskConcurrency; i++ {
		composer.addFileWaitGroup.Add(1)
		composer.errorGroup.Go(func() error {
			return composer.addFileWorker(composer.addFileQueue)
		})
	}
	return composer, nil
}

func (c *GpTarBallComposer) AddFile(info *ComposeFileInfo) {
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
	c.tarFileSets.AddFile(tarBall.Name(), fileInfoHeader.Name)
	c.files.AddFile(fileInfoHeader, info, false)
	return tarBall.TarWriter().WriteHeader(fileInfoHeader)
}

func (c *GpTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *GpTarBallComposer) FinishComposing() (TarFileSets, error) {
	close(c.addFileQueue)

	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}

	c.addFileWaitGroup.Wait()

	err = internal.UploadDto(c.uploader.UploadingFolder, c.aoFiles, getAOFilesMetadataPath(c.backupName))
	if err != nil {
		return nil, fmt.Errorf("failed to upload AO files metadata: %v", err)
	}
	return c.tarFileSets, nil
}

func (c *GpTarBallComposer) GetFiles() BundleFiles {
	return c.files
}

func (c *GpTarBallComposer) addFileWorker(tasks <-chan *ComposeFileInfo) error {
	for task := range tasks {
		err := c.addFile(task)
		if err != nil {
			tracelog.ErrorLogger.Printf(
				"Received an error while adding the file %s: %v", task.path, err)
			return err
		}
	}
	c.addFileWaitGroup.Done()
	return nil
}

func (c *GpTarBallComposer) addFile(cfi *ComposeFileInfo) error {
	// WAL-G uploads AO/AOCS relfiles to different location
	if isAo, meta, location := c.relStorageMap.getAOStorageMetadata(cfi.path); isAo {
		return c.addAOFile(cfi, meta, location)
	}

	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return err
	}
	tarBall.SetUp(c.crypter)
	c.tarFileSetsMutex.Lock()
	c.tarFileSets.AddFile(tarBall.Name(), cfi.header.Name)
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

func (c *GpTarBallComposer) addAOFile(cfi *ComposeFileInfo, aoMeta AoRelFileMetadata, location *walparser.BlockLocation) error {
	storageKey := makeAoFileStorageKey(aoMeta.relNameMd5, aoMeta.modCount, location)
	if _, exists := c.baseAoFiles[storageKey]; exists {
		c.addAoFileMetadata(cfi, storageKey, aoMeta, true)
		tracelog.DebugLogger.Printf("Skipping %s AO relfile (already exists in storage as %s)", cfi.path, storageKey)

		// add reference for the current backup to the storage
		return storeBackupReference(c.uploader.UploadingFolder, storageKey, c.backupName)
	}

	tracelog.DebugLogger.Printf("Uploading %s AO relfile to %s (does not exist in storage)", cfi.path, storageKey)
	fileReadCloser, err := startReadingFile(cfi.header, cfi.fileInfo, cfi.path)
	if err != nil {
		switch err.(type) {
		case FileNotExistError:
			// File was deleted before opening. We should ignore file here as if it did not exist.
			tracelog.WarningLogger.Println(err)
			return nil
		default:
			return err
		}
	}
	defer fileReadCloser.Close()

	// do not compress AO/AOCS segment files since they are already compressed in most cases
	// TODO: lookup the compression details for each relation and compress it when compression is turned off
	var compressor compression.Compressor = nil

	uploadContents := internal.CompressAndEncrypt(fileReadCloser, compressor, c.crypter)
	uploadPath := path.Join(AoStoragePath, storageKey)
	err = c.uploader.Upload(uploadPath, uploadContents)
	if err != nil {
		return err
	}

	c.addAoFileMetadata(cfi, storageKey, aoMeta, false)

	// add reference for the current backup to the storage
	return storeBackupReference(c.uploader.UploadingFolder, storageKey, c.backupName)
}

func (c *GpTarBallComposer) addAoFileMetadata(cfi *ComposeFileInfo, storageKey string, aoMeta AoRelFileMetadata, isSkipped bool) {
	c.aoFilesMutex.Lock()
	c.aoFiles.addFile(cfi.header.Name, storageKey, cfi.fileInfo.ModTime(), aoMeta, cfi.header.Mode, isSkipped)
	c.aoFilesMutex.Unlock()
	c.files.AddFile(cfi.header, cfi.fileInfo, false)
}
