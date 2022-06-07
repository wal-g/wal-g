package archive

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type MongoMetaConstructor struct {
	ctx       context.Context
	client    client.MongoDriver
	folder    storage.Folder
	meta      models.BackupMeta
	permanent bool
}

func (m *MongoMetaConstructor) MetaInfo() interface{} {
	meta := m.Meta()
	backupSentinel := &models.Backup{
		StartLocalTime:  meta.StartTime,
		FinishLocalTime: meta.FinishTime,
		UserData:        meta.User,
		MongoMeta:       meta.Mongo,
		DataSize:        meta.DataSize,
		Permanent:       meta.Permanent,
	}
	return backupSentinel
}

func NewBackupMongoMetaConstructor(ctx context.Context,
	mc client.MongoDriver,
	folder storage.Folder,
	permanent bool) internal.MetaConstructor {
	return &MongoMetaConstructor{ctx: ctx, client: mc, folder: folder, permanent: permanent}
}

func (m *MongoMetaConstructor) Init() error {
	lastTS, lastMajTS, err := m.client.LastWriteTS(m.ctx)
	if err != nil {
		return fmt.Errorf("can not initialize backup mongo")
	}

	userData, err := internal.GetSentinelUserData()
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal the provided UserData")
	}

	m.meta = models.BackupMeta{
		StartTime: utility.TimeNowCrossPlatformLocal(),
		Permanent: m.permanent,
		User:      userData,
		Mongo: models.MongoMeta{
			Before: models.NodeMeta{
				LastTS:    lastTS,
				LastMajTS: lastMajTS,
			},
		},
	}
	return nil
}

func (m *MongoMetaConstructor) Finalize(backupName string) error {
	dataSize, err := internal.FolderSize(m.folder, backupName)
	if err != nil {
		return fmt.Errorf("can not get backup size: %+v", err)
	}

	lastTS, lastMajTS, err := m.client.LastWriteTS(m.ctx)
	if err != nil {
		return fmt.Errorf("can not finalize backup mongo")
	}
	m.meta.Mongo.After = models.NodeMeta{
		LastTS:    lastTS,
		LastMajTS: lastMajTS,
	}
	m.meta.FinishTime = utility.TimeNowCrossPlatformLocal()
	m.meta.DataSize = dataSize
	return nil
}

func (m *MongoMetaConstructor) Meta() models.BackupMeta {
	return m.meta
}
