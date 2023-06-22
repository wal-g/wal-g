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
	"github.com/wal-g/wal-g/pkg/daemon"
	"github.com/wal-g/wal-g/utility"
)

const (
	SdNotifyWatchdog = "WATCHDOG=1"
)

type DaemonOptions struct {
	Uploader *WalUploader
	Reader   internal.StorageFolderReader
}

type SocketMessageHandler interface {
	Handle(messageBody []byte) error
}

type CheckMessageHandler struct {
	fd net.Conn
}

func (h *CheckMessageHandler) Handle(_ []byte) error {
	_, err := h.fd.Write(daemon.OkType.ToBytes())
	if err != nil {
		return fmt.Errorf("failed to write in socket: %w", err)
	}
	tracelog.InfoLogger.Println("Successful configuration check")
	return nil
}

type ArchiveMessageHandler struct {
	fd       net.Conn
	uploader *WalUploader
}

func (h *ArchiveMessageHandler) Handle(messageBody []byte) error {
	tracelog.InfoLogger.Printf("wal file name: %s\n", string(messageBody))

	fullPath, err := getFullPath(path.Join("pg_wal", string(messageBody)))
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("starting wal-push for %s\n", fullPath)
	err = HandleWALPush(h.uploader, fullPath)
	if err != nil {
		return fmt.Errorf("file archiving failed: %w", err)
	}
	_, err = h.fd.Write(daemon.OkType.ToBytes())
	if err != nil {
		return fmt.Errorf("socket write failed: %w", err)
	}
	return nil
}

type WalFetchMessageHandler struct {
	fd     net.Conn
	reader internal.StorageFolderReader
}

func (h *WalFetchMessageHandler) Handle(messageBody []byte) error {
	args, err := daemon.BytesToArgs(messageBody)
	if err != nil {
		return err
	}
	if len(args) != 2 {
		return fmt.Errorf("wal-fetch incorrect arguments count")
	}
	fullPath, err := getFullPath(args[1])
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("starting wal-fetch: %v -> %v\n", args[0], fullPath)

	err = HandleWALFetch(h.reader, args[0], fullPath, true)
	if err != nil {
		return fmt.Errorf("WAL fetch failed: %w", err)
	}
	_, err = h.fd.Write(daemon.OkType.ToBytes())
	if err != nil {
		return fmt.Errorf("socket write failed: %w", err)
	}
	tracelog.InfoLogger.Printf("successfully fetched: %v -> %v\n", args[0], fullPath)
	return nil
}

func NewMessageHandler(messageType daemon.SocketMessageType, c net.Conn, opts DaemonOptions) SocketMessageHandler {
	switch messageType {
	case daemon.CheckType:
		return &CheckMessageHandler{c}
	case daemon.WalPushType:
		return &ArchiveMessageHandler{c, opts.Uploader}
	case daemon.WalFetchType:
		return &WalFetchMessageHandler{c, opts.Reader}
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
func (r SocketMessageReader) Next() (messageType daemon.SocketMessageType, messageBody []byte, err error) {
	messageParameters := make([]byte, 3)
	_, err = io.ReadFull(r.c, messageParameters)
	if err != nil {
		return daemon.ErrorType, nil, fmt.Errorf("failed to read params: %w", err)
	}
	messageType = daemon.SocketMessageType(messageParameters[0])
	var messageLength uint16
	l := bytes.NewReader(messageParameters[1:3])
	err = binary.Read(l, binary.BigEndian, &messageLength)
	if err != nil {
		return daemon.ErrorType, nil, fmt.Errorf("fail to read message len: %w", err)
	}
	messageBody = make([]byte, messageLength-3)
	_, err = io.ReadFull(r.c, messageBody)
	if err != nil {
		return daemon.ErrorType, nil, fmt.Errorf("failed to read msg body: %w", err)
	}
	return messageType, messageBody, err
}

// HandleDaemon is invoked to perform daemon mode
func HandleDaemon(options DaemonOptions, pathToSocket string) {
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
		tracelog.ErrorLogger.PrintOnError(err)
		fd, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Fatal("Failed to accept, err:", err)
		}
		go Listen(fd, options)
	}
}

// Listen is used for listening connection and processing messages
func Listen(c net.Conn, opts DaemonOptions) {
	defer utility.LoggedClose(c, fmt.Sprintf("Failed to close connection with %s \n", c.RemoteAddr()))
	messageReader := NewMessageReader(c)
	for {
		messageType, messageBody, err := messageReader.Next()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to read message from %s, err: %v\n", c.RemoteAddr(), err)
			_, err = c.Write(daemon.ErrorType.ToBytes())
			tracelog.ErrorLogger.PrintOnError(err)
			return
		}
		messageHandler := NewMessageHandler(messageType, c, opts)
		if messageHandler == nil {
			tracelog.ErrorLogger.Printf("Unexpected message type: %s", string(messageType))
			_, err = c.Write(daemon.ErrorType.ToBytes())
			tracelog.ErrorLogger.PrintOnError(err)
			return
		}
		err = messageHandler.Handle(messageBody)
		if err != nil {
			tracelog.ErrorLogger.Println("Failed to handle message:", err)
			_, err = c.Write(daemon.ErrorType.ToBytes())
			tracelog.ErrorLogger.PrintOnError(err)
			return
		}
		if messageType == daemon.WalPushType {
			tracelog.InfoLogger.Printf("Successful archiving for %s\n", string(messageBody))
			return
		}
		if messageType == daemon.WalFetchType {
			return
		}
	}
}

func SdNotify(state string) error {
	socketName, ok := internal.GetSetting(internal.SystemdNotifySocket)
	if !ok {
		return nil
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

func getFullPath(relativePath string) (string, error) {
	PgDataSettingString, ok := internal.GetSetting(internal.PgDataSetting)
	if !ok {
		return "", fmt.Errorf("PGDATA is not set in the conf")
	}
	return path.Join(PgDataSettingString, relativePath), nil
}
