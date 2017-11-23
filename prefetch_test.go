package walg

import "testing"

type MockCleaner struct {
	deleted []string
}

func (this *MockCleaner) GetFiles(directory string) (files []string, err error) {
	files = []string{
		"000000010000000100000056",
		"000000010000000100000057",
		"000000010000000100000058",
		"000000010000000100000059",
		"00000001000000010000005A",
	}
	return
}

func (this *MockCleaner) Remove(file string) {
	this.deleted = append(this.deleted, file)
}

func TestCleanup(t *testing.T) {
	cleaner := MockCleaner{}
	cleanupPrefetchDirectories("000000010000000100000058", "/A", &cleaner)

	if contains(&cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000058") ||
		contains(&cleaner.deleted, "/A/.wal-g/prefetch/running/000000010000000100000059") ||
		contains(&cleaner.deleted, "/A/.wal-g/prefetch/00000001000000010000005A") {
		t.Fatal("Prefetch cleaner deleted wrong files")
	}

	if !contains(&cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000056") ||
		!contains(&cleaner.deleted, "/A/.wal-g/prefetch/running/000000010000000100000056") ||
		!contains(&cleaner.deleted, "/A/.wal-g/prefetch/000000010000000100000057") ||
		!contains(&cleaner.deleted, "/A/.wal-g/prefetch/running/000000010000000100000057") {
		t.Fatal("Prefetch cleaner didnot deleted files")
	}
}
