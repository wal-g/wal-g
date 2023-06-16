package daemon

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

func (msg SocketMessageType) IsEqual(value byte) bool {
	return byte(msg) == value
}
