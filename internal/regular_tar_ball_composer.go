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
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter
	files         *RegularBundleFiles
	tarFileSets   TarFileSets
	errorGroup    *errgroup.Group
}

func NewRegularTarBallComposer(
	tarBallQueue *TarBallQueue,
	tarBallFilePacker *TarBallFilePacker,
	files *RegularBundleFiles,
	crypter crypto.Crypter,
) *RegularTarBallComposer {
	errorGroup, _ := errgroup.WithContext(context.Background())
	return &RegularTarBallComposer{
		tarBallQueue:  tarBallQueue,
		tarFilePacker: tarBallFilePacker,
		crypter:       crypter,
		files:         files,
		tarFileSets:   make(TarFileSets),
		errorGroup:    errorGroup,
	}
}

type RegularTarBallComposerMaker struct {
	filePackerOptions TarBallFilePackerOptions
}

func NewRegularTarBallComposerMaker(filePackerOptions TarBallFilePackerOptions) *RegularTarBallComposerMaker {
	return &RegularTarBallComposerMaker{filePackerOptions: filePackerOptions}
}

func (maker *RegularTarBallComposerMaker) Make(bundle *Bundle) (TarBallComposer, error) {
	bundleFiles := &RegularBundleFiles{}
	tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, bundleFiles, maker.filePackerOptions)
	return NewRegularTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, bundleFiles, bundle.Crypter), nil
}

func (c *RegularTarBallComposer) AddFile(info *ComposeFileInfo) {
	tarBall := c.tarBallQueue.Deque()
	tarBall.SetUp(c.crypter)
	c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], info.header.Name)
	c.errorGroup.Go(func() error {
		err := c.tarFilePacker.PackFileIntoTar(info, tarBall)
		if err != nil {
			return err
		}
		return c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
	})
}

func (c *RegularTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) error {
	tarBall := c.tarBallQueue.Deque()
	tarBall.SetUp(c.crypter)
	defer c.tarBallQueue.EnqueueBack(tarBall)
	c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], fileInfoHeader.Name)
	c.files.AddFile(fileInfoHeader, info, false)
	return tarBall.TarWriter().WriteHeader(fileInfoHeader)
}

func (c *RegularTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RegularTarBallComposer) PackTarballs() (TarFileSets, error) {
	err := c.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return c.tarFileSets, nil
}

func (c *RegularTarBallComposer) GetFiles() BundleFiles {
	return c.files
}
