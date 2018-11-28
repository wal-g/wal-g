package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

type MockCleaner struct {
	deleted []string
}

func (cl *MockCleaner) GetFiles(directory string) (files []string, err error) {
	files = []string{
		"000000010000000100000056",
		"000000010000000100000057",
		"000000010000000100000058",
		"000000010000000100000059",
		"00000001000000010000005A",
	}
	return
}

func (cl *MockCleaner) Remove(file string) {
	cl.deleted = append(cl.deleted, file)
}

func TestCleanup(t *testing.T) {
	cleaner := MockCleaner{}
	internal.CleanupPrefetchDirectories("000000010000000100000058", "/A", &cleaner)

	assert.NotContains(t, cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000058")
	assert.NotContains(t, cleaner.deleted, "/A/.wal-g/prefetch/running/000000010000000100000059")
	assert.NotContains(t, cleaner.deleted, "/A/.wal-g/prefetch/00000001000000010000005A")

	assert.Contains(t, cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000056")
	assert.Contains(t, cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000056")
	assert.Contains(t, cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000056")
	assert.Contains(t, cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000056")
}
