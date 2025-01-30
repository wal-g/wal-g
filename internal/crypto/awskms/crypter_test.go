package awskms

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
)

type MockSymmetricKey struct {
	SymmetricKey
}

func (symmetricKey *MockSymmetricKey) Encrypt() error {
	salt := "152 random bytes to imitate aws kms encryption method, random words here: witch collapse practice feed shame open despair creek road again ice least it!"
	symmetricKey.SetEncryptedKey(append(symmetricKey.GetKey(), salt...))
	return nil
}

func (symmetricKey *MockSymmetricKey) Decrypt() error {
	symmetricKey.SetKey(symmetricKey.GetEncryptedKey()[:symmetricKey.GetKeyLen()])
	return nil
}

func NewMockSymmetricKey(kmsKeyID string, keyLen int, encryptedKeyLen int) *MockSymmetricKey {
	return &MockSymmetricKey{SymmetricKey{SymmetricKeyLen: keyLen, EncryptedSymmetricKeyLen: encryptedKeyLen, KeyID: kmsKeyID}}
}

func MockCrypterFromKeyID(CseKmsID string) crypto.Crypter {
	return &Crypter{SymmetricKey: NewMockSymmetricKey(CseKmsID, 32, 184)}
}

func TestEncryptionCycle(t *testing.T) {
	const someSecret = "so very secret thingy"

	CseKmsID := "AWSKMSKEYID"

	crypter := MockCrypterFromKeyID(CseKmsID)

	buf := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(buf)
	assert.NoErrorf(t, err, "Encryption error: %v", err)

	encrypt.Write([]byte(someSecret))
	encrypt.Close()

	decrypt, err := crypter.Decrypt(buf)
	assert.NoErrorf(t, err, "Decryption error: %v", err)

	decryptedBytes, err := io.ReadAll(decrypt)
	assert.NoErrorf(t, err, "Decryption read error: %v", err)

	assert.Equal(t, someSecret, string(decryptedBytes), "Decrypted text not equals open text")
}
