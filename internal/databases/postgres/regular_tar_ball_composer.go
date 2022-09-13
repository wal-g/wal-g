package postgres

import (
	"archive/tar"
	"context"
	"os"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type RegularTarBallComposer struct {
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePackerImpl
	crypter       crypto.Crypter
	files         internal.BundleFiles
	tarFileSets   internal.TarFileSets
	errorGroup    *errgroup.Group
	ctx           context.Context
}

func NewRegularTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	tarBallFilePacker *TarBallFilePackerImpl,
	files internal.BundleFiles,
	tarFileSets internal.TarFileSets,
	crypter crypto.Crypter,
) *RegularTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	return &RegularTarBallComposer{
		tarBallQueue:  tarBallQueue,
		tarFilePacker: tarBallFilePacker,
		crypter:       crypter,
		files:         files,
		tarFileSets:   tarFileSets,
		errorGroup:    errorGroup,
		ctx:           ctx,
	}
}

type RegularTarBallComposerMaker struct {
	filePackerOptions TarBallFilePackerOptions
	files             internal.BundleFiles
	tarFileSets       internal.TarFileSets
}

func NewRegularTarBallComposerMaker(
	filePackerOptions TarBallFilePackerOptions, files internal.BundleFiles, tarFileSets internal.TarFileSets,
) *RegularTarBallComposerMaker {
	return &RegularTarBallComposerMaker{
		filePackerOptions: filePackerOptions,
		files:             files,
		tarFileSets:       tarFileSets,
	}
}

func (maker *RegularTarBallComposerMaker) Make(bundle *Bundle) (internal.TarBallComposer, error) {
	bundleFiles := maker.files
	tarFileSets := maker.tarFileSets
	tarBallFilePacker := NewTarBallFilePacker(bundle.DeltaMap,
		bundle.IncrementFromLsn, bundleFiles, maker.filePackerOptions)
	return NewRegularTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, bundleFiles, tarFileSets, bundle.Crypter), nil
}

func (c *RegularTarBallComposer) AddFile(info *internal.ComposeFileInfo) {
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return
	}
	tarBall.SetUp(c.crypter)
	c.tarFileSets.AddFile(tarBall.Name(), info.Header.Name)
	c.errorGroup.Go(func() error {
		err := c.tarFilePacker.PackFileIntoTar(info, tarBall)
		if err != nil {
			return err
		}
		return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
	})
}

func (c *RegularTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
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

func (c *RegularTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RegularTarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}

func (c *RegularTarBallComposer) GetFiles() internal.BundleFiles {
	return c.files
}
