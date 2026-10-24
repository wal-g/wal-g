package binary

import (
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/fileparts"
)

func newTarBallComposerMaker() (*fileparts.TarBallComposerMaker, error) {
	value, ok := conf.GetSetting(conf.MongoDBBackupFilePartSize)
	if !ok {
		value = "0"
	}
	partSize, err := strconv.ParseInt(value, 10, 64)
	if err != nil || partSize < 0 {
		return nil, errors.Errorf("%s must be a non-negative integer number of bytes", conf.MongoDBBackupFilePartSize)
	}
	return fileparts.NewTarBallComposerMaker(NewDirDatabaseTarBallComposerMaker(), partSize, shouldSplitWiredTigerFile), nil
}

func shouldSplitWiredTigerFile(info *internal.ComposeFileInfo) bool {
	name := path.Base(info.Header.Name)
	return strings.HasSuffix(name, ".wt") &&
		(strings.HasPrefix(name, "collection-") || strings.HasPrefix(name, "index-"))
}
