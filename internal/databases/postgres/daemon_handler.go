package postgres

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

type SocketMessageType byte

const (
	CheckType    SocketMessageType = 'C'
	FileNameType SocketMessageType = 'F'
)

type SocketMessageHandler interface {
	Handle(messageBody []byte) error
}

type ArchiveMessageHandler struct {
	messageType SocketMessageType
	fd          net.Conn
	uploader    *WalUploader
}

func (h *CheckMessageHandler) Handle(messageBody []byte) error {
	_, err := h.fd.Write([]byte{'O'})
	if err != nil {
		tracelog.ErrorLogger.Printf("Error on writing in socket: %v \n", err)
		return err
	}
	tracelog.InfoLogger.Println("Successful configuration check")
	return nil
}

type CheckMessageHandler struct {
	messageType SocketMessageType
	fd          net.Conn
	uploader    *WalUploader
}

func (h *ArchiveMessageHandler) Handle(messageBody []byte) error {
	err := messageValidation(messageBody)
	if err != nil {
		tracelog.ErrorLogger.Printf("Incorrect message: %v\n", err)
		return err
	}
	tracelog.InfoLogger.Printf("wal file name: %s\n", string(messageBody))
	PgDataSettingString, ok := internal.GetSetting(internal.PgDataSetting)
	if !ok {
		tracelog.ErrorLogger.Print("\nPGDATA is not set in the conf.\n")
		return fmt.Errorf("PGDATA is not set in the conf")
	}
	pathToWal := path.Join(PgDataSettingString, "pg_wal")
	fullPath := path.Join(pathToWal, string(messageBody))
	tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
	err = HandleWALPush(h.uploader, fullPath)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to archive file: %s, err: %v \n", string(messageBody), err)
		return err
	}
	_, err = h.fd.Write([]byte{'O'})
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to write in socket: %v\n", err)
		return err
	}
	return nil
}

func messageValidation(messageBody []byte) error {
	if len(messageBody) < 24 {
		if len(messageBody) > 0 {
			tracelog.ErrorLogger.Println("incorrect message accepted")
			return fmt.Errorf("incorrect message accepted: %s", string(messageBody))
		}
		return fmt.Errorf("empty message accepted")
	}
	return nil
}

func GetMessageHandler(messageType byte, c net.Conn, uploader *WalUploader) SocketMessageHandler {
	switch messageType {
	case 'C':
		return &CheckMessageHandler{CheckType, c, uploader}
	case 'F':
		return &ArchiveMessageHandler{FileNameType, c, uploader}
	default:
		return nil
	}
}

type SocketMessageReader struct {
	c net.Conn
}

func GetMessageReader(c net.Conn) *SocketMessageReader {
	return &SocketMessageReader{c}
}

// Next method reads messages sequentially from the Reader
func (r SocketMessageReader) Next() (messageType byte, messageBody []byte, err error) {
	messageParameters := make([]byte, 3)
	_, err = io.ReadFull(r.c, messageParameters)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read from socket, err: %v \n", err)
		return 'E', nil, err
	}
	messageType = messageParameters[0]
	var messageLength uint16
	l := bytes.NewReader(messageParameters[1:3])
	err = binary.Read(l, binary.BigEndian, &messageLength)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read message length, err: %v \n", err)
		return 'E', nil, err
	}
	messageBody = make([]byte, messageLength-3)
	_, err = io.ReadFull(r.c, messageBody)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to read from socket, err: %v \n", err)
		return 'E', nil, err
	}
	return messageType, messageBody, err
}

// HandleDaemon is invoked to perform daemon mode
func HandleDaemon(uploader *WalUploader, pathToSocket string) {
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
		go DaemonProcess(fd, uploader)
	}
}

// DaemonProcess reads data from connection and processes it
func DaemonProcess(c net.Conn, uploader *WalUploader) {
	defer utility.LoggedClose(c, fmt.Sprintf("Failed to close connection with %s \n", c.RemoteAddr()))
	messageReader := GetMessageReader(c)
	for {
		messageType, messageBody, err := messageReader.Next()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to read  message from client %s, err: %v\n", c.RemoteAddr(), err)
			_, _ = c.Write([]byte{'E'})
			return
		}
		messageHandler := GetMessageHandler(messageType, c, uploader)
		if messageHandler == nil {
			tracelog.ErrorLogger.Printf("Unexpected message type: %s", string(messageType))
			_, _ = c.Write([]byte{'E'})
			return
		}
		err = messageHandler.Handle(messageBody)
		if err != nil {
			tracelog.ErrorLogger.Println("Failed to handle message, err:", err)
			_, _ = c.Write([]byte{'E'})
			return
		}
		if messageType == 'F' {
			return
		}
	}
}
