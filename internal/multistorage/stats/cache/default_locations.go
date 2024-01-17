package cache

import (
	"os"
	"path/filepath"

	"github.com/wal-g/tracelog"
)

// DefaultROMem is the default in-memory cache for read-only statuses that is shared within a single WAL-G process.
var DefaultROMem *SharedMemory

// DefaultRWMem is the default in-memory cache for read-write statuses that is shared within a single WAL-G process.
var DefaultRWMem *SharedMemory

// DefaultROFile is the default file for storing cache on disk that is shared between all WAL-G processes and commands.
var DefaultROFile *SharedFile

// DefaultRWFile is the default file for storing cache on disk that is shared between all WAL-G processes and commands.
var DefaultRWFile *SharedFile

func init() {
	DefaultROMem = NewSharedMemory()
	DefaultRWMem = NewSharedMemory()
	DefaultROFile = NewSharedFile(pathInHomeOrTmp(".walg_storage_ro_status_cache"))
	DefaultRWFile = NewSharedFile(pathInHomeOrTmp(".walg_storage_rw_status_cache"))
}

func pathInHomeOrTmp(name string) string {
	homeDir, err := os.UserHomeDir()
	if err == nil {
		return filepath.Join(homeDir, name)
	}
	tmpDir := "/tmp"
	tracelog.DebugLogger.Printf("Failed to get user HOME dir, will use %q instead: %q", tmpDir, err)

	return filepath.Join(tmpDir, name)
}
