package envelope

import (
	"crypto/sha1"
	"fmt"
	"io"

	"github.com/wal-g/tracelog"
)

//go:generate mockery --name Enveloper --with-expecter=true
type Enveloper interface {
	Name() string
	ReadEncryptedKey(io.Reader) (*EncryptedKey, error)
	DecryptKey(*EncryptedKey) ([]byte, error)
	SerializeEncryptedKey(*EncryptedKey) []byte
}

type EncryptedKey struct {
	id   string
	Data []byte
}

func (encryptedKey *EncryptedKey) ID() string {
	if encryptedKey.id != "" {
		return encryptedKey.id
	}
	uid := encryptedKey.KeyUID()
	tracelog.WarningLogger.Printf("Encrypted key has no ID, UID %s will be used", uid)
	return uid
}

func (encryptedKey *EncryptedKey) KeyUID() string {
	return fmt.Sprintf("sha1:%x", sha1.Sum(encryptedKey.Data))
}

func (encryptedKey *EncryptedKey) WithID(id string) *EncryptedKey {
	return NewEncryptedKey(id, encryptedKey.Data)
}

func NewEncryptedKey(id string, data []byte) *EncryptedKey {
	return &EncryptedKey{id: id, Data: data}
}
