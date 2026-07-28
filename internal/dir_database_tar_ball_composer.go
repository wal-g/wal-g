package internal

import (
	"archive/tar"
	"context"
	"os"
	"path"

	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type PathFilter func(path string) bool

type DirDatabaseTarBallComposer struct {
	// Packing stuff
	files             BundleFiles
	tarBallQueue      *TarBallQueue
	tarBallFilePacker TarBallFilePacker
	tarFileSets       TarFileSets
	crypter           crypto.Crypter
	//
	pathFilter        PathFilter
	fileDirCollection map[string][]*ComposeFileInfo
	ctx               context.Context //nolint:containedctx // errgroup root feeds async addListToTar during filepath.Walk
}

func NewDirDatabaseTarBallComposer(
	ctx context.Context,
	files BundleFiles,
	tarBallQueue *TarBallQueue,
	tarBallFilePacker TarBallFilePacker,
	sets TarFileSets,
	crypter crypto.Crypter,
	pathFilter PathFilter,
) *DirDatabaseTarBallComposer {
	return &DirDatabaseTarBallComposer{
		files:             files,
		tarBallQueue:      tarBallQueue,
		tarFileSets:       sets,
		tarBallFilePacker: tarBallFilePacker,
		fileDirCollection: make(map[string][]*ComposeFileInfo),
		crypter:           crypter,
		pathFilter:        pathFilter,
		ctx:               ctx,
	}
}

func (d DirDatabaseTarBallComposer) AddFile(info *ComposeFileInfo) {
	if d.pathFilter(info.Path) {
		d.fileDirCollection[path.Dir(info.Path)] = append(d.fileDirCollection[path.Dir(info.Path)], info)
	} else {
		d.fileDirCollection[""] = append(d.fileDirCollection[""], info)
	}
}

func (d DirDatabaseTarBallComposer) AddHeader(header *tar.Header, fileInfo os.FileInfo) error {
	tarBall, err := d.tarBallQueue.Deque(d.ctx)
	if err != nil {
		return err
	}
	tarBall.SetUp(d.ctx, d.crypter)
	defer d.tarBallQueue.EnqueueBack(tarBall)
	d.tarFileSets.AddFile(tarBall.Name(), header.Name)
	d.files.AddFile(header, fileInfo, false)
	return tarBall.TarWriter().WriteHeader(header)
}

func (d DirDatabaseTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	d.files.AddSkippedFile(tarHeader, fileInfo)
}

func (d DirDatabaseTarBallComposer) FinishComposing() (TarFileSets, error) {
	// Push Headers in first part
	err := d.addListToTar(make([]*ComposeFileInfo, 0))
	if err != nil {
		return nil, err
	}

	eg, ctx := errgroup.WithContext(d.ctx)
	for _, fileInfos := range d.fileDirCollection {
		thisInfos := fileInfos
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

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

func (d DirDatabaseTarBallComposer) GetFiles() BundleFiles {
	return d.files
}

func (d DirDatabaseTarBallComposer) addListToTar(files []*ComposeFileInfo) error {
	tarBall, err := d.tarBallQueue.Deque(d.ctx)
	if err != nil {
		return err
	}
	tarBall.SetUp(d.ctx, d.crypter)

	for _, file := range files {
		d.tarFileSets.AddFile(tarBall.Name(), file.Header.Name)
		err := d.tarBallFilePacker.PackFileIntoTar(d.ctx, file, tarBall)
		if err != nil {
			return err
		}

		if tarBall.Size() > d.tarBallQueue.TarSizeThreshold {
			err := d.tarBallQueue.FinishTarBall(tarBall)
			if err != nil {
				return err
			}
			tarBall, err = d.tarBallQueue.Deque(d.ctx)
			if err != nil {
				return err
			}
			tarBall.SetUp(d.ctx, d.crypter)
		}
	}
	return d.tarBallQueue.FinishTarBall(tarBall)
}
