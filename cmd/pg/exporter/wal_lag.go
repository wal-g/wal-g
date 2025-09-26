package main

import (
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

// CalculateLSNLag calculates the lag between two LSNs in bytes
func CalculateLSNLag(currentLSN, lastArchivedLSN postgres.LSN) uint64 {
	if currentLSN <= lastArchivedLSN {
		return 0 // No lag or archived is ahead
	}
	return uint64(currentLSN - lastArchivedLSN)
}
