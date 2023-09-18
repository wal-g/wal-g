package envelope

import "io"

//go:generate mockery --name Enveloper --with-expecter=true
type Enveloper interface {
	Name() string
	ReadEncryptedKey(io.Reader) (*EncryptedKey, error)
	DecryptKey(*EncryptedKey) ([]byte, error)
	SerializeEncryptedKey(*EncryptedKey) []byte
}

type EncryptedKey struct {
	ID   string
	Data []byte
}

func NewEncryptedKey(id string, data []byte) *EncryptedKey {
	return &EncryptedKey{ID: id, Data: data}
}
