package daemon

import (
	"encoding/binary"
	"fmt"
	"math"
)

type SocketMessageType byte

const (
	CheckType SocketMessageType = 'C'
	OkType    SocketMessageType = 'O'
	ErrorType SocketMessageType = 'E'

	WalPushType  SocketMessageType = 'F'
	WalFetchType SocketMessageType = 'f'
)

var (
	ErrCorruptedCorruptedMessageBody = fmt.Errorf("currepted message body")
)

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
