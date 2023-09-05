package envelope

import "io"

type EnveloperInterface interface {
	GetName() string
	GetEncryptedKey(r io.Reader) ([]byte, error)
	DecryptKey([]byte) ([]byte, error)
	SerializeEncryptedKey(encryptedKey []byte) []byte
}
