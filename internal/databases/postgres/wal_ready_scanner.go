package postgres

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	conf "github.com/wal-g/wal-g/internal/config"
)

// walReadyScanner retains its arithmetic cursor between polls. New contiguous
// WALs are discovered with stat at the usual poll interval; full directory
// scans remain a fallback for gaps, other timelines and non-segment files.
type walReadyScanner struct {
	dir              string
	lastWAL          string
	limit            int32
	lastFullScan     time.Time
	fullScanInterval time.Duration
	readDir          func(string) ([]os.DirEntry, error)
}

func newWALReadyScanner(dir, firstWAL string, limit int32) (*walReadyScanner, error) {
	interval, err := conf.GetDurationSettingDefault(conf.PgWalFullScanInterval, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if interval < 0 {
		return nil, fmt.Errorf("%s must not be negative", conf.PgWalFullScanInterval)
	}
	return &walReadyScanner{dir: filepath.Join(dir, archiveStatusDir), lastWAL: firstWAL,
		limit: limit, fullScanInterval: interval, readDir: os.ReadDir}, nil
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
	// directory just because asynchronous uploads haven't canceled us yet.
	// Zero disables throttling, including the full predicted-batch shortcut.
	if s.fullScanInterval > 0 && (len(files) == int(s.limit) ||
		(!s.lastFullScan.IsZero() && now.Sub(s.lastFullScan) < s.fullScanInterval)) {
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
