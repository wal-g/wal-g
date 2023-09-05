package envelope

import "io"

//go:generate mockery --name Enveloper --with-expecter=true
type Enveloper interface {
	GetName() string
	GetEncryptedKey(r io.Reader) ([]byte, error)
	DecryptKey([]byte) ([]byte, error)
	SerializeEncryptedKey(encryptedKey []byte) []byte
}
