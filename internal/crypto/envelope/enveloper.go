package envelope

import "io"

//go:generate mockery --name Enveloper --with-expecter=true
type Enveloper interface {
	Name() string
	ReadEncryptedKey(r io.Reader) ([]byte, error)
	DecryptKey([]byte) ([]byte, error)
	SerializeEncryptedKey(encryptedKey []byte) []byte
}
