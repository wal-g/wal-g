package archive

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/wal-g/storages/storage"
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

	_, err := fmt.Fprintln(writer, "name\tfinish_local_time\tts_before\tts_after\tdata_size\tpermanent")
	if err != nil {
		return err
	}
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		_, err := fmt.Fprintf(writer,
			"%v\t%v\t%v\t%v\t%d\t%v\n",
			b.BackupName,
			b.FinishLocalTime.Format(time.RFC3339),
			b.MongoMeta.Before.LastMajTS,
			b.MongoMeta.After.LastMajTS,
			b.DataSize,
			b.Permanent)
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

// MongoMetaProvider defines interface to collect backup mongo
type MongoMetaProvider interface {
	Init(permanent bool) error
	Finalize(backupName string) error
	Meta() models.BackupMeta
}

type MongoMetaDBProvider struct {
	ctx       context.Context
	client    client.MongoDriver
	folder    storage.Folder
	meta      models.BackupMeta
	mongo     models.MongoMeta
	permanent bool
}

func NewBackupMetaMongoProvider(ctx context.Context, mc client.MongoDriver, folder storage.Folder) *MongoMetaDBProvider {
	return &MongoMetaDBProvider{ctx: ctx, client: mc, folder: folder}
}

func (m *MongoMetaDBProvider) Init(permanent bool) error {
	m.permanent = permanent

	lastTS, lastMajTS, err := m.client.LastWriteTS(m.ctx)
	if err != nil {
		return fmt.Errorf("can not initialize backup mongo")
	}
	m.mongo.Before = models.NodeMeta{
		LastTS:    lastTS,
		LastMajTS: lastMajTS,
	}
	return nil
}

func (m *MongoMetaDBProvider) Finalize(backupName string) error {
	dataSize, err := FolderSize(m.folder, backupName)
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
	m.meta = models.BackupMeta{
		Mongo:     m.mongo,
		DataSize:  dataSize,
		Permanent: m.permanent,
		User:      internal.GetSentinelUserData(),
	}
	return nil
}

func (m *MongoMetaDBProvider) Meta() models.BackupMeta {
	return m.meta
}

func FolderSize(folder storage.Folder, path string) (int64, error) {
	dataObjects, _, err := folder.GetSubFolder(path).ListFolder()
	if err != nil {
		return 0, err
	}
	var size int64
	for _, obj := range dataObjects {
		size += obj.GetSize()
	}
	return size, nil
}
