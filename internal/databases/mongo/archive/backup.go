package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// BackupInfoMarshalFunc defines sentinel unmarshal func
type BackupInfoMarshalFunc func(b models.Backup) ([]byte, error)

type BackupListing interface {
	Backups(backups []models.Backup, output io.Writer) error
	Names(backups []internal.BackupTime, output io.Writer) error
}

type TabbedBackupListing struct {
	minwidth int
	tabwidth int
	padding  int
	padchar  byte
	flags    uint
}

func NewDefaultTabbedBackupListing() *TabbedBackupListing {
	return NewTabbedBackupListing(0, 0, 1, ' ', 0)
}

func NewTabbedBackupListing(minwidth, tabwidth, padding int, padchar byte, flags uint) *TabbedBackupListing {
	return &TabbedBackupListing{minwidth, tabwidth, padding, padchar, flags}
}

func (bl *TabbedBackupListing) Backups(backups []models.Backup, output io.Writer) error {
	writer := tabwriter.NewWriter(output, bl.minwidth, bl.tabwidth, bl.padding, bl.padchar, bl.flags)

	_, err := fmt.Fprintln(writer, "name\tfinish_local_time\tts_before\tts_after\tdata_size\tpermanent\tuser_data")
	if err != nil {
		return err
	}
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		var rawUserData []byte
		rawUserData, err = json.Marshal(b.UserData)
		if err != nil {
			rawUserData = []byte("<marshall_error>")
		}

		_, err := fmt.Fprintf(writer,
			"%v\t%v\t%v\t%v\t%d\t%v\t%s\n",
			b.BackupName,
			b.FinishLocalTime.Format(time.RFC3339),
			b.MongoMeta.Before.LastMajTS,
			b.MongoMeta.After.LastMajTS,
			b.DataSize,
			b.Permanent,
			rawUserData,
		)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

func (bl *TabbedBackupListing) Names(backups []internal.BackupTime, output io.Writer) error {
	writer := tabwriter.NewWriter(output, bl.minwidth, bl.tabwidth, bl.padding, bl.padchar, bl.flags)

	// wal_segment_backup_start for backward compatibility
	if _, err := fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start"); err != nil {
		return err
	}
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		_, err := fmt.Fprintf(writer, "%v\t%v\t%v\n", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

type MongoMetaConstructor struct {
	ctx       context.Context
	client    client.MongoDriver
	folder    storage.Folder
	meta      models.BackupMeta
	mongo     models.MongoMeta
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
	m.mongo.Before = models.NodeMeta{
		LastTS:    lastTS,
		LastMajTS: lastMajTS,
	}

	m.meta = models.BackupMeta{
		StartTime: utility.TimeNowCrossPlatformLocal(),
		Mongo:     m.mongo,
		Permanent: m.permanent,
		User:      internal.GetSentinelUserData(),
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
	m.mongo.After = models.NodeMeta{
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
