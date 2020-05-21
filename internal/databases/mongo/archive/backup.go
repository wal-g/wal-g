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
)

// BackupInfoMarshalFunc defines sentinel unmarshal func
type BackupInfoMarshalFunc func(b Backup) ([]byte, error)

type BackupListing interface {
	Backups(backups []Backup, output io.Writer) error
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

func (bl *TabbedBackupListing) Backups(backups []Backup, output io.Writer) error {
	writer := tabwriter.NewWriter(output, bl.minwidth, bl.tabwidth, bl.padding, bl.padchar, bl.flags)

	_, err := fmt.Fprintln(writer, "name\tfinish_local_time\tts_before\tts_after")
	if err != nil {
		return err
	}
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		_, err := fmt.Fprintln(writer,
			fmt.Sprintf("%v\t%v\t%v\t%v", b.BackupName, b.FinishLocalTime.Format(time.RFC3339), b.MongoMeta.Before.LastMajTS, b.MongoMeta.After.LastMajTS))
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
		_, err := fmt.Fprintln(writer,
			fmt.Sprintf("%v\t%v\t%v", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName))
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

// Backup represents backup sentinel data
type Backup struct {
	BackupName      string      `json:"BackupName,omitempty"`
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
	MongoMeta       MongoMeta   `json:"MongoMeta,omitempty"`
}

// NodeMeta represents MongoDB node metadata
type NodeMeta struct {
	LastTS    models.Timestamp `json:"LastTS,omitempty"`
	LastMajTS models.Timestamp `json:"LastMajTS,omitempty"`
}

// MongoMeta includes NodeMeta Before and after backup
type MongoMeta struct {
	Before NodeMeta `json:"Before,omitempty"`
	After  NodeMeta `json:"After,omitempty"`
}

// MongoMetaProvider defines interface to collect backup meta
type MongoMetaProvider interface {
	Init() error
	Finalize() error
	Meta() MongoMeta
}

type MongoMetaDBProvider struct {
	ctx    context.Context
	client client.MongoDriver
	meta   MongoMeta
}

func NewBackupMetaMongoProvider(ctx context.Context, client client.MongoDriver) *MongoMetaDBProvider {
	return &MongoMetaDBProvider{ctx: ctx, client: client}
}

func (m *MongoMetaDBProvider) Init() error {
	lastTS, lastMajTS, err := m.client.LastWriteTS(m.ctx)
	if err != nil {
		return fmt.Errorf("can not initialize backup meta")
	}
	m.meta.Before = NodeMeta{
		LastTS:    lastTS,
		LastMajTS: lastMajTS,
	}
	return nil
}

func (m *MongoMetaDBProvider) Finalize() error {
	lastTS, lastMajTS, err := m.client.LastWriteTS(m.ctx)
	if err != nil {
		return fmt.Errorf("can not finalize backup meta")
	}
	m.meta.After = NodeMeta{
		LastTS:    lastTS,
		LastMajTS: lastMajTS,
	}
	return nil
}

func (m *MongoMetaDBProvider) Meta() MongoMeta {
	return m.meta
}
