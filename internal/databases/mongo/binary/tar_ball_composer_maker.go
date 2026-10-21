package binary

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
)

func newTarBallComposerMaker() (internal.TarBallComposerMaker, error) {
	value, ok := conf.GetSetting(conf.MongoDBBackupFilePartSize)
	if !ok {
		return NewDirDatabaseTarBallComposerMaker(), nil
	}
	partSize, err := strconv.ParseInt(value, 10, 64)
	if err != nil || partSize < 0 {
		return nil, errors.Errorf("%s must be a non-negative integer number of bytes", conf.MongoDBBackupFilePartSize)
	}
	if partSize == 0 {
		return NewDirDatabaseTarBallComposerMaker(), nil
	}
	return NewFilePartsTarBallComposerMaker(partSize), nil
}
