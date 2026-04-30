package binary

import (
	"github.com/wal-g/wal-g/internal"
	"strings"
)

const (
	CollectionPrefix = "collection"
	IndexPrefix      = "index"
)

type DirDatabaseTarBallComposerMaker struct {
	files       internal.BundleFiles
	tarFileSets internal.TarFileSets
}

func NewDirDatabaseTarBallComposerMaker() *DirDatabaseTarBallComposerMaker {
	return &DirDatabaseTarBallComposerMaker{
		files:       &internal.RegularBundleFiles{},
		tarFileSets: internal.NewRegularTarFileSets(),
	}
}

func (maker *DirDatabaseTarBallComposerMaker) Make(bundle *internal.Bundle) (internal.TarBallComposer, error) {
	packer := internal.NewRegularTarBallFilePacker(maker.files, false)
	return internal.NewDirDatabaseTarBallComposer(
		maker.files,
		bundle.TarBallQueue,
		packer,
		maker.tarFileSets,
		bundle.Crypter,
		mongoPathFilter,
	), nil
}

func mongoPathFilter(path string) bool {
	return strings.Contains(path, CollectionPrefix) || strings.Contains(path, IndexPrefix)
}
