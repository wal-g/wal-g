package postgres

import (
	"net"

	"github.com/wal-g/tracelog"
)

// HandleDaemon is invoked to perform daemon mode
func HandleDaemon(c net.Conn, f func(string) error) {
	defer func(c net.Conn) {
		err := c.Close()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to close connection with %s, err: %v\n", c.RemoteAddr(), err)
		}
	}(c)
	buf := make([]byte, 512)
	nr, err := c.Read(buf)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read checking message from client %s, err: %v\n", c.RemoteAddr(), err)
		_, _ = c.Write([]byte("READ_FAILED"))
		return
	}
	if nr == 5 && string(buf[0:5]) == "CHECK" {
		_, _ = c.Write([]byte("CHECKED"))
		tracelog.InfoLogger.Printf("Successful configuration check")
	} else {
		tracelog.ErrorLogger.Printf("Error on configuration check")
		return
	}
	n, err := c.Read(buf)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read message with file from client %s, err: %v\n", c.RemoteAddr(), err)
		_, _ = c.Write([]byte("READ_FAILED"))
		return
	}

	if n < 24 {
		if n > 0 {
			tracelog.ErrorLogger.Printf("Received incorrect message %s from %s", buf[0:n], c.RemoteAddr())
		} else {
			tracelog.ErrorLogger.Printf("Received empty message from %s", c.RemoteAddr())
		}
		_, _ = c.Write([]byte("BAD_MSG"))
		return
	}

	data := buf[0:24]
	tracelog.InfoLogger.Printf("wal file name: %s\n", string(data))

	err = f(string(data))
	if err != nil {
		tracelog.ErrorLogger.Printf("wal-push failed: %v\n", err)
		_, _ = c.Write([]byte("FAIL"))
		return
	}

	_, err = c.Write([]byte("OK"))
	if err != nil {
		tracelog.ErrorLogger.Println("OK write fail: ", err)
	}
}
