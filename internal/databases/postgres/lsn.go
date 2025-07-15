package postgres

import (
	"fmt"

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

func lsnMin(a, b LSN) LSN {
	if a < b {
		return a
	}
	return b
}
