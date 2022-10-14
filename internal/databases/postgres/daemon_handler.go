package postgres

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"path"

	"github.com/wal-g/tracelog"
)

// HandleDaemon is invoked to perform daemon mode
func HandleDaemon(uploader *WalUploader, pathToSocket string, pathToWal string) {
	_ = os.Remove(pathToSocket)
	l, err := net.Listen("unix", pathToSocket)
	if err != nil {
		tracelog.ErrorLogger.Fatal("Error on listening socket:", err)
	}
	for {
		fd, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Println("Failed to accept, err:", err)
		}

		go CheckDaemon(fd, func(walFileName string) error {
			fullPath := path.Join(pathToWal, walFileName)
			tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
			return HandleWALPush(uploader, fullPath)
		})
	}
}

// CheckDaemon is invoked to check all needs of archiving
func CheckDaemon(c net.Conn, f func(string) error) {
	defer func(c net.Conn) {
		err := c.Close()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to close connection with %s, err: %v\n", c.RemoteAddr(), err)
		}
	}(c)
	buf := make([]byte, 512)
	for {
		nr, err := c.Read(buf)
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to read checking message from client %s, err: %v\n", c.RemoteAddr(), err)
			_, _ = c.Write([]byte("READ_FAILED"))
			return
		}

		requestType, requestLength, requestBody := BufferParser(buf)
		response := ValidateMessage(requestType, requestLength, requestBody, f)
		_, _ = c.Write([]byte(response))
		if response == "OK" {
			return
		}
		if int(requestLength) < nr {
			requestType, requestLength, requestBody = BufferParser(buf[requestLength:])
			response = ValidateMessage(requestType, requestLength, requestBody, f)
			_, _ = c.Write([]byte(response))
			return
		}
	}
}

// BufferParser is invoked to read bytes from buffer
func BufferParser(buf []byte) (string, uint16, string) {
	messageType := string(buf[0])
	tracelog.InfoLogger.Printf("request type is : %s", messageType)

	var messageLength uint16
	l := bytes.NewReader(buf[1:3])
	err := binary.Read(l, binary.BigEndian, &messageLength)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read message length, err: %v", err)
	}
	tracelog.InfoLogger.Printf("request len is : %d", messageLength)
	messageBody := string(buf[3 : messageLength-1])
	tracelog.InfoLogger.Printf("request body is : %s", messageBody)
	return messageType, messageLength, messageBody
}

// ValidateMessage is invoked to validate request and to give a correct response
func ValidateMessage(requestType string, requestLength uint16, requestBody string, f func(string) error) string {
	if requestType == "C" {
		if requestBody == "CHECK" {
			tracelog.InfoLogger.Println("Successful configuration check")
			return "CHECKED"
		}
		tracelog.ErrorLogger.Printf("Incorrect message body: '%s', expected: '%s'", requestBody, "CHECK")
		return "BAD_MSG"
	}
	if requestType == "F" {
		if requestLength < 28 {
			if requestLength > 0 {
				tracelog.ErrorLogger.Printf("Received incorrect message %s", requestBody)
			} else {
				tracelog.ErrorLogger.Println("Received empty message")
			}
			return "BAD_MSG"
		}
		tracelog.InfoLogger.Printf("wal file name: %s\n", requestBody)
		err := f(requestBody)
		if err != nil {
			tracelog.ErrorLogger.Printf("wal-push failed: %v\n", err)
			return "FAIL"
		}
		return "OK"
	}
	tracelog.ErrorLogger.Println("Incorrect message type")
	return "BAD_MSG"
}
