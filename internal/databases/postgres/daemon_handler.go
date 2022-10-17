package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path"

	"github.com/wal-g/tracelog"
)

type CheckType byte
type FileType byte

type MessageSocketHandler interface {
	Handle(messageBody []byte, c net.Conn, f func(string) error) error
}

func MessageTypeConstruct(messageType byte) MessageSocketHandler {
	switch messageType {
	case 'C':
		return CheckType('C')
	case 'F':
		return FileType('F')
	default:
		return nil
	}
}

func (msg CheckType) Handle(messageBody []byte, c net.Conn, f func(string) error) error {
	_, err := c.Write([]byte{'O', 0, 3})
	if err != nil {
		tracelog.ErrorLogger.Printf("Error on writing in socket: %v", err)
		return err
	}
	return nil
}

func (msg FileType) Handle(messageBody []byte, c net.Conn, f func(string) error) error {
	if len(messageBody) < 24 {
		if len(messageBody) > 0 {
			tracelog.ErrorLogger.Printf("Received incorrect message %s", messageBody)
		} else {
			tracelog.ErrorLogger.Println("Received empty message")
		}
		return errors.New(fmt.Sprint("Incorrect message accepted"))
	}
	tracelog.InfoLogger.Printf("wal file name: %s\n", messageBody)
	err := f(string(messageBody))
	if err != nil {
		tracelog.ErrorLogger.Printf("wal-push failed: %v\n", err)
		return err
	}
	_, err = c.Write([]byte{'O', 0, 3})
	if err != nil {
		return err
	}
	return nil
}

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
			_, _ = c.Write([]byte{'E', 0, 3})
			return
		}

		byteType, byteLength, byteBody := MessageParser(buf)
		requestType := MessageTypeConstruct(byteType)
		err = requestType.Handle(byteBody, c, f)
		if err != nil {
			tracelog.ErrorLogger.Println("Failed to handle message, err:", err)
		}
		if byteType == 'F' {
			return
		}
		if int(byteLength) < nr {
			tracelog.InfoLogger.Println("Read remaining buffer...")
			byteType, byteLength, byteBody = MessageParser(buf[byteLength:])
			err = requestType.Handle(byteBody, c, f)
			if err != nil {
				tracelog.ErrorLogger.Println("Failed to handle message, err:", err)
			}
			return
		}
	}
}

// MessageParser is invoked to read bytes from buffer
func MessageParser(buf []byte) (byte, uint16, []byte) {
	messageType := buf[0]
	var messageLength uint16
	l := bytes.NewReader(buf[1:3])
	err := binary.Read(l, binary.BigEndian, &messageLength)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read message length, err: %v", err)
	}
	messageBody := buf[3 : messageLength-1]
	return messageType, messageLength, messageBody
}
