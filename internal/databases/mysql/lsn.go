package mysql

import (
	"fmt"
	"strconv"
)

type LSN uint64

func (lsn LSN) String() string {
	return fmt.Sprintf("%v", uint64(lsn))
}

func ParseLSN(s string) (LSN, error) {
	lsn, err := strconv.ParseUint(s, 0, 64) // FIXME: should we support hex?

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
