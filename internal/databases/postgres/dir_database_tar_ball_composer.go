package postgres

import (
	"archive/tar"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/sync/errgroup"
	"os"
	"path"
	"strings"
)

type DirDatabaseTarBallComposerMaker struct {
	filePackerOptions TarBallFilePackerOptions
	files             internal.BundleFiles
	tarFileSets       internal.TarFileSets
}

func NewDirDatabaseTarBallComposerMaker(files internal.BundleFiles, filePackerOptions TarBallFilePackerOptions, tarFileSets internal.TarFileSets) *DirDatabaseTarBallComposerMaker {
	return &DirDatabaseTarBallComposerMaker{
		files:             files,
		filePackerOptions: filePackerOptions,
		tarFileSets:       tarFileSets,
	}
}

func (m DirDatabaseTarBallComposerMaker) Make(bundle *Bundle) (internal.TarBallComposer, error) {
	tarPacker := NewTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, m.files, m.filePackerOptions)
	return newDirDatabaseTarBallComposer(
		m.files,
		bundle.TarBallQueue,
		tarPacker,
		m.tarFileSets,
		bundle.Crypter,
	), nil
}

type DirDatabaseTarBallComposer struct {
	// Packing stuff
	files         internal.BundleFiles
	tarBallQueue  *internal.TarBallQueue
	tarFilePacker *TarBallFilePackerImpl
	tarFileSets   internal.TarFileSets
	crypter       crypto.Crypter
	//
	fileDirCollection map[string][]*internal.ComposeFileInfo
}

func newDirDatabaseTarBallComposer(
	files internal.BundleFiles,
	tarBallQueue *internal.TarBallQueue,
	tarFilePacker *TarBallFilePackerImpl,
	sets internal.TarFileSets,
	crypter crypto.Crypter,
) *DirDatabaseTarBallComposer {

	return &DirDatabaseTarBallComposer{
		files:             files,
		tarBallQueue:      tarBallQueue,
		tarFilePacker:     tarFilePacker,
		tarFileSets:       sets,
		fileDirCollection: make(map[string][]*internal.ComposeFileInfo),
		crypter:           crypter,
	}
}

func (d DirDatabaseTarBallComposer) AddFile(info *internal.ComposeFileInfo) {
	if strings.Contains(info.Path, "base") {
		d.fileDirCollection[path.Dir(info.Path)] = append(d.fileDirCollection[path.Dir(info.Path)], info)
	} else {
		d.fileDirCollection[""] = append(d.fileDirCollection[""], info)
	}
}

func (d DirDatabaseTarBallComposer) AddHeader(header *tar.Header, fileInfo os.FileInfo) error {
	tarBall := d.tarBallQueue.Deque()
	tarBall.SetUp(d.crypter)
	defer d.tarBallQueue.EnqueueBack(tarBall)
	d.tarFileSets.AddFile(tarBall.Name(), header.Name)
	d.files.AddFile(header, fileInfo, false)
	return tarBall.TarWriter().WriteHeader(header)
}

func (d DirDatabaseTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	d.files.AddSkippedFile(tarHeader, fileInfo)
}

func (d DirDatabaseTarBallComposer) FinishComposing() (internal.TarFileSets, error) {
	eg := errgroup.Group{}
	for _, fileInfos := range d.fileDirCollection {
		thisInfos := fileInfos
		eg.Go(func() error { return d.addListToTar(thisInfos) })
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return d.tarFileSets, nil
}

func (d DirDatabaseTarBallComposer) GetFiles() internal.BundleFiles {
	return d.files
}

func (d DirDatabaseTarBallComposer) addListToTar(files []*internal.ComposeFileInfo) error {
	tarBall := d.tarBallQueue.Deque()
	tarBall.SetUp(d.crypter)

	for _, file := range files {

		d.tarFileSets.AddFile(tarBall.Name(), file.Header.Name)
		err := d.tarFilePacker.PackFileIntoTar(file, tarBall)
		if err != nil {
			return err
		}

		if tarBall.Size() > d.tarBallQueue.TarSizeThreshold {
			err := d.tarBallQueue.FinishTarBall(tarBall)
			if err != nil {
				return err
			}
			tarBall = d.tarBallQueue.Deque()
			tarBall.SetUp(d.crypter)
		}
	}
	return d.tarBallQueue.FinishTarBall(tarBall)
}
