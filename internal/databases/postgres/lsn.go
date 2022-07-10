package postgres

import "github.com/jackc/pgx"

type LSN uint64

func (lsn LSN) String() string {
	return pgx.FormatLSN(uint64(lsn))
}

func ParseLSN(s string) (LSN, error) {
	lsn, err := pgx.ParseLSN(s)
	if err != nil {
		return 0, nil
	}

	return LSN(lsn), nil
}

func lsnMin(a, b LSN) LSN {
	if a < b {
		return a
	}
	return b
}
