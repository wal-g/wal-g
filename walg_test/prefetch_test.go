package walg_test

import (
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
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
	walg.CleanupPrefetchDirectories("000000010000000100000058", "/A", &cleaner)

	if testtools.Contains(&cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000058") ||
		testtools.Contains(&cleaner.deleted, "/A/.wal-g/prefetch/running/000000010000000100000059") ||
		testtools.Contains(&cleaner.deleted, "/A/.wal-g/prefetch/00000001000000010000005A") {
		t.Fatal("Prefetch cleaner deleted wrong files")
	}

	if !testtools.Contains(&cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000056") ||
		!testtools.Contains(&cleaner.deleted, "/A/.wal-g/prefetch/running/000000010000000100000056") ||
		!testtools.Contains(&cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000057") ||
		!testtools.Contains(&cleaner.deleted, "/A/.wal-g/prefetch/running/000000010000000100000057") {
		t.Fatal("Prefetch cleaner didnot deleted files")
	}
}
