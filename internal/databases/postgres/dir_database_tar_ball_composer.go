package postgres

import (
	"strings"

	"github.com/wal-g/wal-g/internal"
)

type DirDatabaseTarBallComposerMaker struct {
	filePackerOptions TarBallFilePackerOptions
	files             internal.BundleFiles
	tarFileSets       internal.TarFileSets
}

func NewDirDatabaseTarBallComposerMaker(files internal.BundleFiles, filePackerOptions TarBallFilePackerOptions,
	tarFileSets internal.TarFileSets) *DirDatabaseTarBallComposerMaker {
	return &DirDatabaseTarBallComposerMaker{
		files:             files,
		filePackerOptions: filePackerOptions,
		tarFileSets:       tarFileSets,
	}
}

func (m DirDatabaseTarBallComposerMaker) Make(bundle *Bundle) (internal.TarBallComposer, error) {
	tarPacker := NewTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, m.files, m.filePackerOptions)
	return internal.NewDirDatabaseTarBallComposer(
		m.files,
		bundle.TarBallQueue,
		tarPacker,
		m.tarFileSets,
		bundle.Crypter,
		postgresPathFilter,
	), nil
}

func postgresPathFilter(path string) bool {
	return strings.Contains(path, DefaultTablespace)
}
