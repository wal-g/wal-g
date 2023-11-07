package mysql

import (
	"fmt"
	"strconv"
)

type LSN uint64

func (lsn LSN) String() string {
	return fmt.Sprintf("%v", uint64(lsn))
}

func ParseLSN(s string) *LSN {
	lsn, err := strconv.ParseUint(s, 0, 64)

	if err != nil {
		return nil
	}

	var result = LSN(lsn)
	return &result
}
