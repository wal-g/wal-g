package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/wal-g/wal-g/internal"
	client "github.com/wal-g/wal-g/internal/databases/redis/client"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	RDBBackupType = "rdb"
	AOFBackupType = "aof"
)

// Backup represents backup sentinel data
type Backup struct {
	BackupName      string      `json:"BackupName,omitempty"`
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
	Permanent       bool        `json:"Permanent"`
	DataSize        int64       `json:"DataSize,omitempty"`
	BackupSize      int64       `json:"BackupSize,omitempty"`
	BackupType      string      `json:"BackupType,omitempty"`
	Version         string      `json:"Version,omitempty"`
	UsedMemory      int64       `json:"UsedMemory,omitempty"`
	UsedMemoryRss   int64       `json:"UsedMemoryRss,omitempty"`
}

func (b Backup) Name() string {
	return b.BackupName
}

func (b Backup) StartTime() time.Time {
	return b.StartLocalTime
}

func (b Backup) IsPermanent() bool {
	return b.Permanent
}

func (b Backup) IsAOF() bool {
	return b.BackupType == AOFBackupType
}

func (b Backup) IsRDB() bool {
	return b.BackupType == RDBBackupType
}

func (b Backup) VersionStr() string {
	return b.Version
}

func (b Backup) PrintableFields() []printlist.TableField {
	prettyStartTime := internal.PrettyFormatTime(b.StartLocalTime)
	prettyFinishTime := internal.PrettyFormatTime(b.FinishLocalTime)
	return []printlist.TableField{
		{
			Name:       "name",
			PrettyName: "Name",
			Value:      b.BackupName,
		},
		{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       internal.FormatTime(b.StartLocalTime),
			PrettyValue: &prettyStartTime,
		},
		{
			Name:        "finish_time",
			PrettyName:  "Finish time",
			Value:       internal.FormatTime(b.FinishLocalTime),
			PrettyValue: &prettyFinishTime,
		},
		{
			Name:       "user_data",
			PrettyName: "UserData",
			Value:      marshalUserData(b.UserData),
		},
		{
			Name:       "data_size",
			PrettyName: "Data size",
			Value:      strconv.FormatInt(b.DataSize, 10),
		},
		{
			Name:       "backup_size",
			PrettyName: "Backup size",
			Value:      strconv.FormatInt(b.BackupSize, 10),
		},
		{
			Name:       "permanent",
			PrettyName: "Permanent",
			Value:      fmt.Sprintf("%v", b.Permanent),
		},
		{
			Name:       "backup_type",
			PrettyName: "Backup type",
			Value:      b.BackupType,
		},
		{
			Name:       "version",
			PrettyName: "Backup version",
			Value:      b.Version,
		},
		{
			Name:       "used_memory",
			PrettyName: "Used memory (limited by maxmemory)",
			Value:      strconv.FormatInt(b.UsedMemory, 10),
		},
		{
			Name:       "used_memory_rss",
			PrettyName: "Used memory (as seen by OS))",
			Value:      strconv.FormatInt(b.UsedMemoryRss, 10),
		},
	}
}

func (b Backup) ToInternal(folder storage.Folder) internal.Backup {
	return internal.Backup{
		Folder: folder.GetSubFolder(utility.BaseBackupPath),
		Name:   b.BackupName,
	}
}

func marshalUserData(userData interface{}) string {
	rawUserData, err := json.Marshal(userData)
	if err != nil {
		rawUserData = []byte(fmt.Sprintf("{\"error\": \"unable to marshal %+v\"}", userData))
	}
	return string(rawUserData)
}

func SplitRedisBackups(backups []Backup, purgeBackups, retainBackups map[string]bool) (purge, retain []Backup) {
	for i := range backups {
		backup := backups[i]
		if purgeBackups[backup.Name()] {
			purge = append(purge, backup)
			continue
		}
		if retainBackups[backup.Name()] {
			retain = append(retain, backup)
		}
	}
	return purge, retain
}

func RedisModelToTimedBackup(backups []Backup) []internal.TimedBackup {
	if backups == nil {
		return nil
	}
	result := make([]internal.TimedBackup, len(backups))
	for i := range backups {
		result[i] = backups[i]
	}
	return result
}

// BackupMeta stores the data needed to create a Backup json object
type BackupMeta struct {
	DataSize       int64
	CompressedSize int64
	Permanent      bool
	User           interface{}
	StartTime      time.Time
	FinishTime     time.Time
	BackupType     string
	Version        string
	UsedMemory     int64
	UsedMemoryRss  int64
}

type RedisMetaConstructor struct {
	ctx              context.Context
	folder           storage.Folder
	meta             BackupMeta
	permanent        bool
	backupType       string
	versionParser    *VersionParser
	memoryDataGetter client.MemoryDataGetter
}

// Init - required for internal.MetaConstructor
func (m *RedisMetaConstructor) Init() error {
	userData, err := internal.GetSentinelUserData()
	if err != nil {
		return err
	}
	memData := m.memoryDataGetter.Get()
	m.meta = BackupMeta{
		Permanent:     m.permanent,
		User:          userData,
		StartTime:     utility.TimeNowCrossPlatformLocal(),
		BackupType:    m.backupType,
		UsedMemory:    memData.UsedMemory,
		UsedMemoryRss: memData.UsedMemoryRss,
	}
	if m.versionParser != nil {
		m.meta.Version, err = m.versionParser.Get()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *RedisMetaConstructor) MetaInfo() interface{} {
	meta := m.meta
	return &Backup{
		Permanent:       meta.Permanent,
		UserData:        meta.User,
		StartLocalTime:  meta.StartTime,
		FinishLocalTime: meta.FinishTime,
		BackupType:      meta.BackupType,
		Version:         meta.Version,
		UsedMemory:      meta.UsedMemory,
		UsedMemoryRss:   meta.UsedMemoryRss,
	}
}

func (m *RedisMetaConstructor) Finalize(backupName string) error {
	m.meta.FinishTime = utility.TimeNowCrossPlatformLocal()
	return nil
}

func NewBackupRedisMetaConstructor(ctx context.Context, folder storage.Folder, permanent bool, backupType string,
	versionParser *VersionParser, memoryDataGetter client.MemoryDataGetter) internal.MetaConstructor {
	return &RedisMetaConstructor{
		ctx: ctx, folder: folder,
		permanent:        permanent,
		backupType:       backupType,
		versionParser:    versionParser,
		memoryDataGetter: memoryDataGetter,
	}
}

func SentinelWithExistenceCheck(folder storage.Folder, backupName string) (Backup, error) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return Backup{}, err
	}

	return fetchSentinel(backup, backupName)
}

func SentinelWithoutExistenceCheck(folder storage.Folder, backupName string) (Backup, error) {
	backup, err := internal.NewBackup(folder, backupName)
	if err != nil {
		return Backup{}, err
	}

	return fetchSentinel(backup, backupName)
}

func fetchSentinel(backup internal.Backup, backupName string) (Backup, error) {
	var sentinel Backup
	if err := backup.FetchSentinel(&sentinel); err != nil {
		return Backup{}, err
	}
	if sentinel.BackupName == "" {
		sentinel.BackupName = backupName
	}
	if sentinel.BackupType == "" {
		sentinel.BackupType = RDBBackupType
	}
	return sentinel, nil
}
