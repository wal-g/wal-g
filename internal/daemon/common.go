package daemon

import (
	"os"
	"time"
	"net"

	"encoding/binary"
	"fmt"
	"context"
	"math"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

type SocketMessageType byte

const (
	CheckType               SocketMessageType = 'C'
	OkType                  SocketMessageType = 'O'
	ErrorType               SocketMessageType = 'E'
	ArchiveNonExistenceType SocketMessageType = 'N'

	WalPushType  SocketMessageType = 'F'
	WalFetchType SocketMessageType = 'f'


	STPutType  SocketMessageType = 'P'
	STCatType  SocketMessageType = 'C'
)

var (
	ErrCorruptedCorruptedMessageBody = fmt.Errorf("currepted message body")
)

const (
	SdNotifyWatchdog = "WATCHDOG=1"
)

type DaemonOptions struct {
	SocketPath string
}

type SocketMessageHandler interface {
	Handle(ctx context.Context, messageBody []byte) error
}

type DaemonListener interface {
	// Listen is used for listening connection and processing messages
	Listen(ctx context.Context, c net.Conn)
}

func (msg SocketMessageType) ToBytes() []byte {
	return []byte{byte(msg)}
}

func (msg SocketMessageType) IsEqual(value byte) bool {
	return byte(msg) == value
}

func ArgsToBytes(args ...string) ([]byte, error) {
	argsLen := len(args)
	if argsLen > 255 {
		return nil, fmt.Errorf("unsupported args count")
	}
	res := []byte{byte(argsLen)}
	for _, v := range args {
		if len(v) > math.MaxUint16 {
			return nil, fmt.Errorf("unsupported arg size")
		}
		res = binary.BigEndian.AppendUint16(res, uint16(len(v)))
		res = append(res, []byte(v)...)
		if len(res) > math.MaxUint16-1-2*argsLen {
			return nil, fmt.Errorf("unsupported total args size")
		}
	}
	return res, nil
}

func BytesToArgs(body []byte) ([]string, error) {
	argsCount := int(body[0])
	res := make([]string, 0, argsCount)
	idx := 1
	for i := 0; i < argsCount; i++ {
		if idx+2 >= len(body) {
			return nil, ErrCorruptedCorruptedMessageBody
		}
		l := int(binary.BigEndian.Uint16(body[idx : idx+2]))
		idx += 2
		if idx+l > len(body) {
			return nil, ErrCorruptedCorruptedMessageBody
		}
		res = append(res, string(body[idx:idx+l]))
		idx += l
	}
	if len(body) != idx {
		return nil, ErrCorruptedCorruptedMessageBody
	}
	return res, nil
}



func SendSdNotify(c <-chan time.Time) {
	for {
		<-c
		tracelog.ErrorLogger.PrintOnError(SdNotify(SdNotifyWatchdog))
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
		return NewSocketWriteFailedError(err)
	}
	return nil
}


// HandleDaemon is invoked to perform daemon mode
func HandleDaemon(options DaemonOptions, dl DaemonListener) {
	if _, err := os.Stat(options.SocketPath); err == nil {
		err = os.Remove(options.SocketPath)
		if err != nil {
			tracelog.ErrorLogger.Fatal("Failed to remove socket file:", err)
		}
	}
	l, err := net.Listen("unix", options.SocketPath)
	if err != nil {
		tracelog.ErrorLogger.Fatal("Error on listening socket:", err)
	}

	sdNotifyTicker := time.NewTicker(30 * time.Second)
	defer sdNotifyTicker.Stop()
	go SendSdNotify(sdNotifyTicker.C)

	for {
		fd, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Fatal("Failed to accept, err:", err)
		}
		go dl.Listen(context.Background(), fd)
	}
}