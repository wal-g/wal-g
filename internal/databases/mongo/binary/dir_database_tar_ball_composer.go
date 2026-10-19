package binary

import (
	"context"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

const (
	CollectionPrefix = "collection"
	IndexPrefix      = "index"
)

type DirDatabaseTarBallComposerMaker struct {
	files       internal.BundleFiles
	tarFileSets internal.TarFileSets
	fileParts   map[string]models.FilePartsInfo
}

func NewDirDatabaseTarBallComposerMaker() *DirDatabaseTarBallComposerMaker {
	return &DirDatabaseTarBallComposerMaker{
		files:       &internal.RegularBundleFiles{},
		tarFileSets: internal.NewRegularTarFileSets(),
		fileParts:   make(map[string]models.FilePartsInfo),
	}
}

func (maker *DirDatabaseTarBallComposerMaker) Make(ctx context.Context, bundle *internal.Bundle) (internal.TarBallComposer, error) {
	packer := internal.NewRegularTarBallFilePacker(maker.files, false)
	composer := internal.NewDirDatabaseTarBallComposer(
		ctx,
		maker.files,
		bundle.TarBallQueue,
		packer,
		maker.tarFileSets,
		bundle.Crypter,
		mongoPathFilter,
	)
	value, ok := conf.GetSetting(conf.MongoDBBackupFilePartSize)
	if !ok {
		return composer, nil
	}
	partSize, err := strconv.ParseInt(value, 10, 64)
	if err != nil || partSize < 0 {
		return nil, errors.Errorf("%s must be a non-negative integer number of bytes", conf.MongoDBBackupFilePartSize)
	}
	if partSize == 0 {
		return composer, nil
	}
	return &filePartsTarBallComposer{
		TarBallComposer: composer,
		ctx:             ctx,
		bundle:          bundle,
		sets:            maker.tarFileSets,
		partSize:        partSize,
		fileParts:       maker.fileParts,
	}, nil
}

func mongoPathFilter(path string) bool {
	return strings.Contains(path, CollectionPrefix) || strings.Contains(path, IndexPrefix)
}
