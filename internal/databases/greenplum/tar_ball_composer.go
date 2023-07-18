package greenplum

import (
	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/wal-g/internal"
)

type TarBallComposerType int

const (
	RegularComposer TarBallComposerType = iota + 1
	DatabaseComposer
)

func NewGpTarBallComposerMaker(composerType TarBallComposerType, relStorageMap AoRelFileStorageMap,
	uploader internal.Uploader, backupName string,
) (postgres.TarBallComposerMaker, error) {
	switch composerType {
	case RegularComposer:
		return NewRegularTarBallComposerMaker(relStorageMap, uploader, backupName)
	case DatabaseComposer:
		return NewDirDatabaseTarBallComposerMaker(relStorageMap, uploader, backupName)
	default:
		return nil, errors.New("NewTarBallComposerMaker: Unknown TarBallComposerType")
	}
}
