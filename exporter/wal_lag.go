package main

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN represents a PostgreSQL Log Sequence Number
type LSN uint64

// ParseLSN parses a PostgreSQL LSN string (e.g., "0/1A2B3C4D") into an LSN value
func ParseLSN(lsnStr string) (LSN, error) {
	if lsnStr == "" {
		return 0, fmt.Errorf("empty LSN string")
	}

	// PostgreSQL LSN format: "timeline/offset" where both are hex
	parts := strings.Split(lsnStr, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN format: %s", lsnStr)
	}

	// Parse the timeline part (first part)
	timeline, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid timeline in LSN %s: %v", lsnStr, err)
	}

	// Parse the offset part (second part)
	offset, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid offset in LSN %s: %v", lsnStr, err)
	}

	// Combine timeline and offset into a single 64-bit value
	// PostgreSQL uses 32 bits for timeline and 32 bits for offset
	lsn := LSN((timeline << 32) | offset)
	return lsn, nil
}

// String converts an LSN back to PostgreSQL string format
func (lsn LSN) String() string {
	timeline := uint32(lsn >> 32)
	offset := uint32(lsn & 0xFFFFFFFF)
	return fmt.Sprintf("%X/%X", timeline, offset)
}

// CalculateLSNLag calculates the lag between two LSNs in bytes
func CalculateLSNLag(currentLSN, lastArchivedLSN LSN) uint64 {
	if currentLSN <= lastArchivedLSN {
		return 0 // No lag or archived is ahead
	}
	return uint64(currentLSN - lastArchivedLSN)
}
