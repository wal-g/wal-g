package utility

import (
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
)

func LoggedClose(c io.Closer, errmsg string) {
	err := c.Close()
	if err != nil {
		tracelog.ErrorLogger.Println(errmsg, ": ", err)
	}
}
