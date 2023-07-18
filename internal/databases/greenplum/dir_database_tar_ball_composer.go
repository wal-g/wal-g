package greenplum

import (
	"context"
	"path"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"golang.org/x/sync/errgroup"
)

func NewDirDatabaseTarBallComposerMaker(relStorageMap AoRelFileStorageMap, uploader internal.Uploader, backupName string,
) (*RegularTarBallComposerMaker, error) {
	return &RegularTarBallComposerMaker{
		relStorageMap: relStorageMap,
		bundleFiles:   &internal.RegularBundleFiles{},
		TarFileSets:   internal.NewRegularTarFileSets(),
		uploader:      uploader,
		backupName:    backupName,
		Constructor:   NewDirDatabaseTarBallComposer,
	}, nil
}

type DirDatabaseTarBallComposer struct {
	*RegularTarBallComposer

	fileDirCollection map[string][]*internal.ComposeFileInfo
}

func NewDirDatabaseTarBallComposer(
	tarBallQueue *internal.TarBallQueue, crypter crypto.Crypter, relStorageMap AoRelFileStorageMap,
	bundleFiles internal.BundleFiles, packer *postgres.TarBallFilePackerImpl, aoStorageUploader *AoStorageUploader,
	tarFileSets internal.TarFileSets, uploader internal.Uploader, backupName string,
) (internal.TarBallComposer, error) {
	errorGroup, ctx := errgroup.WithContext(context.Background())

	regularComposer := RegularTarBallComposer{
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
	composer := &DirDatabaseTarBallComposer{
		RegularTarBallComposer: &regularComposer,
		fileDirCollection:      make(map[string][]*internal.ComposeFileInfo),
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

func (d *DirDatabaseTarBallComposer) addFileWorker(tasks <-chan *internal.ComposeFileInfo) error {
	for task := range tasks {
		err := d.addFile(task)
		if err != nil {
			tracelog.ErrorLogger.Printf(
				"Received an error while adding the file %s: %v", task.Path, err)
			return err
		}
	}
	d.addFileWaitGroup.Done()
	return nil
}

func (d *DirDatabaseTarBallComposer) addFile(cfi *internal.ComposeFileInfo) error {
	// WAL-G uploads AO/AOCS relfiles to a different location
	added, err := d.tryAddWithAoStorageManager(cfi)
	if err != nil {
		return err
	}
	if added {
		return nil
	}

	tracelog.DebugLogger.Printf("%s is not an AO/AOCS file, will process it through a regular tar file packer",
		cfi.Path)
	if strings.Contains(cfi.Path, "base") {
		d.fileDirCollection[path.Dir(cfi.Path)] = append(d.fileDirCollection[path.Dir(cfi.Path)], cfi)
	} else {
		d.fileDirCollection[""] = append(d.fileDirCollection[""], cfi)
	}
	return nil
}

func (d *DirDatabaseTarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	_, err := d.RegularTarBallComposer.FinishComposing()

	if err != nil {
		return nil, err
	}

	// Push Headers in first part
	err = d.addListToTar(make([]*internal.ComposeFileInfo, 0))
	if err != nil {
		return nil, err
	}

	eg := errgroup.Group{}
	for _, fileInfos := range d.fileDirCollection {
		thisInfos := fileInfos
		eg.Go(func() error {
			if len(thisInfos) == 0 {
				return nil
			}
			return d.addListToTar(thisInfos)
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return d.tarFileSets, nil
}

func (d *DirDatabaseTarBallComposer) addListToTar(files []*internal.ComposeFileInfo) error {
	tarBallQueue := d.tarBallQueue

	tarBall := tarBallQueue.Deque()
	tarBall.SetUp(d.crypter)

	for _, file := range files {
		d.tarFileSets.AddFile(tarBall.Name(), file.Header.Name)
		err := d.tarFilePacker.PackFileIntoTar(file, tarBall)
		if err != nil {
			return err
		}

		if tarBall.Size() > tarBallQueue.TarSizeThreshold {
			err := tarBallQueue.FinishTarBall(tarBall)
			if err != nil {
				return err
			}
			tarBall = tarBallQueue.Deque()
			tarBall.SetUp(d.crypter)
		}
	}
	return tarBallQueue.FinishTarBall(tarBall)
}
