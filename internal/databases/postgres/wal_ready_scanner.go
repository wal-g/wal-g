package postgres

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

const walReadyFullScanInterval = 5 * time.Second

// walReadyScanner retains its arithmetic cursor between polls. New contiguous
// WALs are discovered with stat at the usual poll interval; full directory
// scans remain a fallback for gaps, other timelines and non-segment files.
type walReadyScanner struct {
	dir          string
	lastWAL      string
	limit        int32
	lastFullScan time.Time
	readDir      func(string) ([]os.DirEntry, error)
}

func newWALReadyScanner(dir, firstWAL string, limit int32) *walReadyScanner {
	return &walReadyScanner{dir: filepath.Join(dir, archiveStatusDir), lastWAL: firstWAL,
		limit: limit, readDir: os.ReadDir}
}

func (s *walReadyScanner) candidates(now time.Time) ([]string, error) {
	var files []string
	for i := int32(0); i < s.limit; i++ {
		next, err := GetNextWalFilename(s.lastWAL)
		if err != nil {
			break
		}
		name := next + readySuffix
		if _, err := os.Stat(filepath.Join(s.dir, name)); err != nil {
			break
		}
		files = append(files, name)
		s.lastWAL = next
	}
	// Enough predicted candidates for a whole batch: don't enumerate the
	// directory just because asynchronous uploads haven't cancelled us yet.
	if len(files) == int(s.limit) || (!s.lastFullScan.IsZero() && now.Sub(s.lastFullScan) < walReadyFullScanInterval) {
		return files, nil
	}
	s.lastFullScan = now
	entries, err := s.readDir(s.dir)
	if err != nil {
		return files, err
	}
	predicted := make(map[string]bool, len(files))
	for _, name := range files {
		predicted[name] = true
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), readySuffix) && !predicted[entry.Name()] {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}
