package postgres

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
)

type LSN uint64

func (lsn LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

func ParseLSN(s string) (LSN, error) {
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		return 0, err
	}

	return LSN(lsn), nil
}

// ParseLSNOrNumber accepts LSN either in PostgreSQL text format (e.g. "0/EDA81980")
// or as a plain decimal number (e.g. "3987216768")
func ParseLSNOrNumber(s string) (LSN, error) {
	if strings.Contains(s, "/") {
		return ParseLSN(s)
	}

	lsn, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN %q: expected format X/X or decimal number", s)
	}

	return LSN(lsn), nil
}
