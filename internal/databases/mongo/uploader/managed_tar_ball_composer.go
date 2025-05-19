package uploader

import (
	"archive/tar"
	"context"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
	"os"
)

type ManagedTarBallComposer struct {
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker internal.TarBallFilePacker
	crypter       crypto.Crypter
	files         internal.BundleFiles
	tarFileSets   internal.TarFileSets
	errorGroup    *errgroup.Group
	ctx           context.Context

	shouldClose bool
}

func NewManagedTarBallComposer(
	tarBallQueue *internal.TarBallQueue,
	tarBallFilePacker internal.TarBallFilePacker,
	files internal.BundleFiles,
	tarFileSets internal.TarFileSets,
	crypter crypto.Crypter,
) *ManagedTarBallComposer {
	errorGroup, ctx := errgroup.WithContext(context.Background())
	return &ManagedTarBallComposer{
		tarBallQueue:  tarBallQueue,
		tarFilePacker: tarBallFilePacker,
		crypter:       crypter,
		files:         files,
		tarFileSets:   tarFileSets,
		errorGroup:    errorGroup,
		ctx:           ctx,
	}
}

type ManagedTarBallComposerMaker struct {
	files       internal.BundleFiles
	tarFileSets internal.TarFileSets
}

func NewManagedTarBallComposerMaker() *ManagedTarBallComposerMaker {
	return &ManagedTarBallComposerMaker{
		files:       &internal.RegularBundleFiles{},
		tarFileSets: internal.NewRegularTarFileSets(),
	}
}

func (maker *ManagedTarBallComposerMaker) Make(bundle *internal.Bundle) (internal.TarBallComposer, error) {
	bundleFiles := maker.files
	tarFileSets := maker.tarFileSets
	packer := internal.NewRegularTarBallFilePacker(bundleFiles)
	return NewManagedTarBallComposer(bundle.TarBallQueue, packer, bundleFiles, tarFileSets, bundle.Crypter), nil
}

func (m *ManagedTarBallComposer) AddFile(info *internal.ComposeFileInfo) {
	tarBall, err := m.tarBallQueue.DequeCtx(m.ctx)
	if err != nil {
		return
	}
	tarBall.SetUp(m.crypter)
	m.tarFileSets.AddFile(tarBall.Name(), info.Header.Name)
	m.errorGroup.Go(func() error {
		err := m.tarFilePacker.PackFileIntoTar(info, tarBall)
		if err != nil {
			return err
		}
		if m.shouldClose {
			m.shouldClose = false
			return m.tarBallQueue.FinishTarBall(tarBall)
		}
		return m.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
	})
}

func (m *ManagedTarBallComposer) AddHeader(header *tar.Header, fileInfo os.FileInfo) error {
	tarBall, err := m.tarBallQueue.DequeCtx(m.ctx)
	if err != nil {
		return m.errorGroup.Wait()
	}
	tarBall.SetUp(m.crypter)
	defer m.tarBallQueue.EnqueueBack(tarBall)
	m.tarFileSets.AddFile(tarBall.Name(), header.Name)
	m.files.AddFile(header, fileInfo, false)
	return tarBall.TarWriter().WriteHeader(header)
}

func (m *ManagedTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	m.files.AddSkippedFile(tarHeader, fileInfo)
}

func (m *ManagedTarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	err := m.errorGroup.Wait()
	if err != nil {
		return nil, err
	}
	return m.tarFileSets, nil
}

func (m *ManagedTarBallComposer) GetFiles() internal.BundleFiles {
	return m.files
}

func (m *ManagedTarBallComposer) CloseOnNext() {
	m.shouldClose = true
}

func (m *ManagedTarBallComposer) GetTarFileSets() map[string][]string {
	return m.tarFileSets.Get()
}

func (m *ManagedTarBallComposer) FinishTarBall() error {
	tarBall, err := m.tarBallQueue.DequeCtx(m.ctx)
	if err != nil {
		return err
	}
	tarBall.SetUp(m.crypter)
	return m.tarBallQueue.FinishTarBall(tarBall)
}
