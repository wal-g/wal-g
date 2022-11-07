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
	OkType       SocketMessageType = 'O'
	ErrorType    SocketMessageType = 'E'
)

func (msg SocketMessageType) ToBytes() []byte {
	return []byte{byte(msg)}
}

type SocketMessageHandler interface {
	Handle(messageBody []byte) error
}

type CheckMessageHandler struct {
	messageType SocketMessageType
	fd          net.Conn
	uploader    *WalUploader
}

func (h *CheckMessageHandler) Handle(messageBody []byte) error {
	_, err := h.fd.Write(OkType.ToBytes())
	if err != nil {
		return fmt.Errorf("failed to write in socket: %w", err)
	}
	tracelog.InfoLogger.Println("Successful configuration check")
	return nil
}

type ArchiveMessageHandler struct {
	messageType SocketMessageType
	fd          net.Conn
	uploader    *WalUploader
}

func (h *ArchiveMessageHandler) Handle(messageBody []byte) error {
	tracelog.InfoLogger.Printf("wal file name: %s\n", string(messageBody))
	PgDataSettingString, ok := internal.GetSetting(internal.PgDataSetting)
	if !ok {
		return fmt.Errorf("PGDATA is not set in the conf")
	}
	pathToWal := path.Join(PgDataSettingString, "pg_wal")
	fullPath := path.Join(pathToWal, string(messageBody))
	tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
	err := HandleWALPush(h.uploader, fullPath)
	if err != nil {
		return fmt.Errorf("file archiving failed: %w", err)
	}
	_, err = h.fd.Write(OkType.ToBytes())
	if err != nil {
		return fmt.Errorf("socket write failed: %w", err)
	}
	return nil
}

func NewMessageHandler(messageType SocketMessageType, c net.Conn, uploader *WalUploader) SocketMessageHandler {
	switch messageType {
	case CheckType:
		return &CheckMessageHandler{CheckType, c, uploader}
	case FileNameType:
		return &ArchiveMessageHandler{FileNameType, c, uploader}
	default:
		return nil
	}
}

type SocketMessageReader struct {
	c net.Conn
}

func NewMessageReader(c net.Conn) *SocketMessageReader {
	return &SocketMessageReader{c}
}

// Next method reads messages sequentially from the Reader
func (r SocketMessageReader) Next() (messageType SocketMessageType, messageBody []byte, err error) {
	messageParameters := make([]byte, 3)
	_, err = io.ReadFull(r.c, messageParameters)
	if err != nil {
		return ErrorType, nil, fmt.Errorf("failed to read params: %w", err)
	}
	messageType = SocketMessageType(messageParameters[0])
	var messageLength uint16
	l := bytes.NewReader(messageParameters[1:3])
	err = binary.Read(l, binary.BigEndian, &messageLength)
	if err != nil {
		return ErrorType, nil, fmt.Errorf("fail to read message len: %w", err)
	}
	messageBody = make([]byte, messageLength-3)
	_, err = io.ReadFull(r.c, messageBody)
	if err != nil {
		return ErrorType, nil, fmt.Errorf("failed to read msg body: %w", err)
	}
	return messageType, messageBody, err
}

// HandleDaemon is invoked to perform daemon mode
func HandleDaemon(uploader *WalUploader, pathToSocket string) {
	if _, err := os.Stat(pathToSocket); err == nil {
		err = os.Remove(pathToSocket)
		if err != nil {
			tracelog.ErrorLogger.Fatal("Failed to remove socket file:", err)
		}
	}
	l, err := net.Listen("unix", pathToSocket)
	if err != nil {
		tracelog.ErrorLogger.Fatal("Error on listening socket:", err)
	}
	for {
		err = SdNotify(SdNotifyWatchdog)
		tracelog.ErrorLogger.FatalOnError(err)

		fd, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Fatal("Failed to accept, err:", err)
		}
		go Listen(fd, uploader)
	}
}

// Listen is used for listening connection and processing messages
func Listen(c net.Conn, uploader *WalUploader) {
	defer utility.LoggedClose(c, fmt.Sprintf("Failed to close connection with %s \n", c.RemoteAddr()))
	messageReader := NewMessageReader(c)
	for {
		messageType, messageBody, err := messageReader.Next()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to read message from %s, err: %v\n", c.RemoteAddr(), err)
			_, err = c.Write(ErrorType.ToBytes())
			tracelog.ErrorLogger.PrintOnError(err)
			return
		}
		messageHandler := NewMessageHandler(messageType, c, uploader)
		if messageHandler == nil {
			tracelog.ErrorLogger.Printf("Unexpected message type: %s", string(messageType))
			_, err = c.Write(ErrorType.ToBytes())
			tracelog.ErrorLogger.PrintOnError(err)
			return
		}
		err = messageHandler.Handle(messageBody)
		if err != nil {
			tracelog.ErrorLogger.Println("Failed to handle message:", err)
			_, err = c.Write(ErrorType.ToBytes())
			tracelog.ErrorLogger.PrintOnError(err)
			return
		}
		if messageType == FileNameType {
			tracelog.InfoLogger.Printf("Successful archiving for %s\n", string(messageBody))
			return
		}
	}
}

const SdNotifyWatchdog = "WATCHDOG=1"

func SdNotify(state string) error {
	socketName, ok := os.LookupEnv("NOTIFY_SOCKET")
	if !ok {
		return fmt.Errorf("NOTIFY_SOCKET is not defined")
	}
	socketAddr := &net.UnixAddr{
		Name: socketName,
		Net:  "unixgram",
	}
	conn, err := net.DialUnix(socketAddr.Net, nil, socketAddr)
	if err != nil {
		return fmt.Errorf("failed connect to service: %w", err)
	}
	defer utility.LoggedClose(conn, fmt.Sprintf("Failed to close connection with %s \n", conn.RemoteAddr()))
	if _, err = conn.Write([]byte(state)); err != nil {
		return fmt.Errorf("failed write to service: %w", err)
	}
	return nil
}
