package internal

import (
	"archive/tar"
	"context"
	"os"

	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
)

type RegularTarBallComposer struct {
	tarBallQueue  *TarBallQueue
	tarFilePacker TarBallFilePacker
	crypter       crypto.Crypter
	files         BundleFiles
	tarFileSets   TarFileSets
	errorGroup    *errgroup.Group
	ctx           context.Context
}

func NewRegularTarBallComposer(
	ctx context.Context,
	tarBallQueue *TarBallQueue,
	tarBallFilePacker TarBallFilePacker,
	files BundleFiles,
	tarFileSets TarFileSets,
	crypter crypto.Crypter,
) *RegularTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(ctx)
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
	files       BundleFiles
	tarFileSets TarFileSets

	skipFileNotExists bool
}

func NewRegularTarBallComposerMaker(files BundleFiles, tarFileSets TarFileSets, skipFileNotExists bool) *RegularTarBallComposerMaker {
	return &RegularTarBallComposerMaker{
		files:             files,
		tarFileSets:       tarFileSets,
		skipFileNotExists: skipFileNotExists,
	}
}

func (maker *RegularTarBallComposerMaker) Make(ctx context.Context, bundle *Bundle) (TarBallComposer, error) {
	bundleFiles := maker.files
	tarFileSets := maker.tarFileSets
	packer := NewRegularTarBallFilePacker(bundleFiles, maker.skipFileNotExists)
	return NewRegularTarBallComposer(ctx, bundle.TarBallQueue, packer, bundleFiles, tarFileSets, bundle.Crypter), nil
}

func (c *RegularTarBallComposer) AddFile(info *ComposeFileInfo) error {
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return err
	}
	if err := tarBall.SetUp(c.ctx, c.crypter); err != nil {
		return err
	}
	c.tarFileSets.AddFile(tarBall.Name(), info.Header.Name)
	c.errorGroup.Go(func() error {
		err := c.tarFilePacker.PackFileIntoTar(info, tarBall)
		if err != nil {
			return err
		}
		return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
	})
	return nil
}

func (c *RegularTarBallComposer) AddHeader(header *tar.Header, fileInfo os.FileInfo) error {
	tarBall, err := c.tarBallQueue.DequeCtx(c.ctx)
	if err != nil {
		return c.errorGroup.Wait()
	}
	if err := tarBall.SetUp(c.ctx, c.crypter); err != nil {
		return err
	}
	defer c.tarBallQueue.EnqueueBack(tarBall)
	c.tarFileSets.AddFile(tarBall.Name(), header.Name)
	c.files.AddFile(header, fileInfo, false)
	return tarBall.TarWriter().WriteHeader(header)
}

func (c *RegularTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RegularTarBallComposer) FinishComposing() (TarFileSets, error) {
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}

func (c *RegularTarBallComposer) GetFiles() BundleFiles {
	return c.files
}
