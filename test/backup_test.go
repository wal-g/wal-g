package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

func TestCheckExistence_Exists(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder, "base_000")
	exists, err := backup.CheckExistence()
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckExistence_NotExists(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder, "base_321")
	exists, err := backup.CheckExistence()
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestGetTarNames(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder, "base_456")
	tarNames, err := backup.GetTarNames()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, tarNames)
}
