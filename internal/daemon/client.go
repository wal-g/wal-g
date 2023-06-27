package daemon

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

type RunOptions struct {
	MessageType SocketMessageType
	SocketName  string
	MessageArgs []string

	DaemonOperationTimeout        time.Duration
	DaemonSocketConnectionTimeout time.Duration
}

func getMessage(messageType SocketMessageType, messageArgs []string) ([]byte, error) {
	switch len(messageArgs) {
	case 0:
		return binary.BigEndian.AppendUint16(messageType.ToBytes(), uint16(3)), nil
	case 1:
		res := binary.BigEndian.AppendUint16(messageType.ToBytes(), uint16(len(messageArgs[0])+3))
		return append(res, []byte(messageArgs[0])...), nil
	}

	messageBody, err := ArgsToBytes(messageArgs...)
	if err != nil {
		return nil, err
	}
	res := binary.BigEndian.AppendUint16(messageType.ToBytes(), uint16(len(messageBody)+3))
	return append(res, messageBody...), nil
}

func SendCommand(opts *RunOptions) (error, SocketMessageType) {
	ctx, cancel := context.WithTimeout(context.Background(), opts.DaemonSocketConnectionTimeout)
	defer cancel()

	dialer := net.Dialer{}
	daemonAddr := net.UnixAddr{Name: opts.SocketName, Net: "unix"}
	socketConnection, err := dialer.DialContext(ctx, "unix", daemonAddr.String())
	if err != nil {
		return fmt.Errorf("unix socket dial error: %w", err), ErrorType
	}
	defer socketConnection.Close()
	err = socketConnection.SetDeadline(time.Now().Add(opts.DaemonOperationTimeout))
	if err != nil {
		return fmt.Errorf("unix socket set deadline error: %w", err), ErrorType
	}

	msg, err := getMessage(opts.MessageType, opts.MessageArgs)
	if err != nil {
		return err, ErrorType
	}
	_, err = socketConnection.Write(msg)
	if err != nil {
		return fmt.Errorf("unix socket write error: %w", err), ErrorType
	}

	resp := make([]byte, 512)
	n, err := socketConnection.Read(resp)
	if err != nil {
		return fmt.Errorf("unix socket read error: %w", err), ErrorType
	}
	if n < 1 || !OkType.IsEqual(resp[0]) {
		return fmt.Errorf("daemon command run error [message type: %v, args: %v, daemon response: %v]",
			string(opts.MessageType), opts.MessageArgs, string(resp[0])), SocketMessageType(resp[0])
	}
	return nil, OkType
}
