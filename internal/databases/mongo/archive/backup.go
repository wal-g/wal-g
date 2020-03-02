package archive

import (
	"context"
	"fmt"
	"time"

	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

// SentinelMarshalFunc defines sentinel unmarshal func
type SentinelMarshalFunc func(dto StreamSentinelDto) ([]byte, error)

// StreamSentinelDto represents backup sentinel data
type StreamSentinelDto struct {
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
	MongoMeta       BackupMeta  `json:"MongoMeta,omitempty"`
}

// NodeMeta represents MongoDB node metadata
type NodeMeta struct {
	LastTS    models.Timestamp `json:"LastTS,omitempty"`
	LastMajTS models.Timestamp `json:"LastMajTS,omitempty"`
}

// BackupMeta includes NodeMeta Before and after backup
type BackupMeta struct {
	Before NodeMeta `json:"Before,omitempty"`
	After  NodeMeta `json:"After,omitempty"`
}

// BackupMetaProvider defines interface to collect backup meta
type BackupMetaProvider interface {
	Init() error
	Finalize() error
	Meta() BackupMeta
}

type BackupMetaMongoProvider struct {
	ctx    context.Context
	client client.MongoDriver
	meta   BackupMeta
}

func NewBackupMetaMongoProvider(ctx context.Context, client client.MongoDriver) *BackupMetaMongoProvider {
	return &BackupMetaMongoProvider{ctx: ctx, client: client}
}

func (m *BackupMetaMongoProvider) Init() error {
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

func (m *BackupMetaMongoProvider) Finalize() error {
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

func (m *BackupMetaMongoProvider) Meta() BackupMeta {
	return m.meta
}
