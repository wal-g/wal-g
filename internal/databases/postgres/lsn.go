package postgres

import (
	"github.com/jackc/pglogrepl"
)

type LSN pglogrepl.LSN

func (lsn LSN) String() string {
	return pglogrepl.LSN(lsn).String()
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
