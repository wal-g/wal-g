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
		go DaemonProcess(fd, func(walFileName string) error {
			fullPath := path.Join(pathToWal, walFileName)
			tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
			return HandleWALPush(uploader, fullPath)
		})
	}
}

func DaemonProcess(c net.Conn, f func(string) error) {
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
		requestType, requestLength, requestBody := MessageParser(buf)
		response := HandleMessage(requestType, requestLength, requestBody, f)
		_, _ = c.Write([]byte(response))
		if response == "OK" {
			return
		}
		if int(requestLength) < nr {
			tracelog.InfoLogger.Println("Read remaining buffer...")
			requestType, requestLength, requestBody = MessageParser(buf[requestLength:])
			response = HandleMessage(requestType, requestLength, requestBody, f)
			_, _ = c.Write([]byte(response))
			return
		}
	}
}

// MessageParser is invoked to read bytes from buffer
func MessageParser(buf []byte) (string, uint16, string) {
	messageType := string(buf[0])
	var messageLength uint16
	l := bytes.NewReader(buf[1:3])
	err := binary.Read(l, binary.BigEndian, &messageLength)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read message length, err: %v", err)
	}
	messageBody := string(buf[3 : messageLength-1])
	return messageType, messageLength, messageBody
}

// HandleMessage is invoked to validate message and to return a correct response
func HandleMessage(messageType string, messageLength uint16, messageBody string, f func(string) error) string {
	if messageType == "C" {
		if messageBody[0:5] == "CHECK" {
			tracelog.InfoLogger.Println("Successful configuration check")
			return "CHECKED"
		}
		tracelog.ErrorLogger.Printf("Incorrect message body: '%s', expected: '%s'", messageBody, "CHECK")
		return "BAD_MSG"
	}
	if messageType == "F" {
		if messageLength < 28 {
			if messageLength > 0 {
				tracelog.ErrorLogger.Printf("Received incorrect message %s", messageBody)
			} else {
				tracelog.ErrorLogger.Println("Received empty message")
			}
			return "BAD_MSG"
		}
		tracelog.InfoLogger.Printf("wal file name: %s\n", messageBody)
		err := f(messageBody)
		if err != nil {
			tracelog.ErrorLogger.Printf("wal-push failed: %v\n", err)
			return "FAIL"
		}
		return "OK"
	}
	tracelog.ErrorLogger.Println("Incorrect message type")
	return "BAD_MSG"
}
