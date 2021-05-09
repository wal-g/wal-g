package yckms

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	testSecretString = "this is a very secret string used in our tests"
)

type mockedSymmetricKey struct {
	key          []byte
	encryptedKey []byte
}

func (m *mockedSymmetricKey) GetKey() []byte {
	return m.key
}

func (m *mockedSymmetricKey) Decrypt() error {
	m.key = make([]byte, 32)
	for i := range m.key {
		m.key[i] = 0xbb
	}
	return nil
}

func (m *mockedSymmetricKey) GetEncryptedKey() []byte {
	return m.encryptedKey
}

func (m *mockedSymmetricKey) ReadEncryptedKey(r io.Reader) error {
	m.encryptedKey = make([]byte, 64)
	_, err := r.Read(m.encryptedKey)
	return err
}

func (m *mockedSymmetricKey) CreateKey() error {
	m.encryptedKey = make([]byte, 64)
	for i := range m.encryptedKey {
		m.encryptedKey[i] = 0xaa
	}
	m.key = make([]byte, 32)
	for i := range m.key {
		m.key[i] = 0xbb
	}
	return nil
}

func MockedYcCrypter() crypto.Crypter {
	return &YcCrypter{
		symmetricKey: &mockedSymmetricKey{
			key:          nil,
			encryptedKey: nil,
		},
	}
}

func TestYcCrypterEncryptionCycle(t *testing.T) {
	crypter := MockedYcCrypter()
	buffer := new(bytes.Buffer)

	encrypt, err := crypter.Encrypt(buffer)
	assert.NoErrorf(t, err, "YcCrypter encryption error: %v", err)

	_, err = encrypt.Write([]byte(testSecretString))
	assert.NoErrorf(t, err, "YcCrypter writing error: %v", err)
	err = encrypt.Close()
	assert.NoErrorf(t, err, "YcCrypter closing error: %v", err)

	decrypt, err := crypter.Decrypt(buffer)
	assert.NoErrorf(t, err, "YcCrypter decryption error: %v", err)

	decryptedData, err := ioutil.ReadAll(decrypt)
	assert.NoErrorf(t, err, "YcCryptor reading decrypted data error: %v", err)

	assert.Equal(t, testSecretString, string(decryptedData), "Decrypted text not equal to plain text")
}
