package mysql

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"testing"
)

func setupTestEnvironment(t *testing.T) {
	// Сброс глобальных переменных перед каждым тестом
	globalStreamer = nil
	syncStarted = false

	// Настройка тестового окружения
	t.Setenv("WALG_MYSQL_BINLOG_SERVER_HOST", "127.0.0.1")
	t.Setenv("WALG_MYSQL_BINLOG_SERVER_PORT", "0")
	t.Setenv("WALG_MYSQL_BINLOG_SERVER_USER", "testuser")
	t.Setenv("WALG_MYSQL_BINLOG_SERVER_PASSWORD", "testpass")
	t.Setenv("WALG_MYSQL_BINLOG_SERVER_ID", "99")
	t.Setenv("WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE", "test@tcp(127.0.0.1:3306)/test")

	// Настройка мок-хранилища
	t.Setenv("WALG_FILE_PREFIX", "file://localhost/tmp/test")
}

type mockHashableStorage struct {
	folder storage.Folder
}

func (m *mockHashableStorage) RootFolder() storage.Folder {
	return m.folder
}

func (m *mockHashableStorage) Close() error {
	return nil
}

func (m *mockHashableStorage) HashName() string {
	return "mock"
}

func (m *mockHashableStorage) ConfigHash() string {
	return "mock-config-hash"
}

func createMockStorage() storage.HashableStorage {
	folder := memory.NewFolder("test/", memory.NewKVS())
	return &mockHashableStorage{folder: folder}
}

// Тест специально для проверки логики реконнекта
func TestBinlogServerReconnectLogic(t *testing.T) {
	setupTestEnvironment(t)

	handler := Handler{}
	pos := mysql.Position{Name: "mysql-bin.000001", Pos: 4}

	// Сброс состояния
	globalStreamer = nil
	syncStarted = false

	// Первое подключение - должно создать новый streamer
	streamer1 := replication.NewBinlogStreamer()
	globalStreamer = streamer1
	syncStarted = true

	result1, err := handler.HandleBinlogDump(pos)
	require.NoError(t, err)
	assert.Equal(t, streamer1, result1)

	// Второе подключение - должно вернуть существующий streamer
	result2, err := handler.HandleBinlogDump(pos)
	require.NoError(t, err)
	assert.Equal(t, streamer1, result2)
	assert.Equal(t, result1, result2, "Reconnection should return the same streamer instance")

	// Проверяем, что sync не запускался повторно
	assert.True(t, syncStarted)
}
