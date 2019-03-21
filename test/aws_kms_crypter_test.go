package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"io/ioutil"
	"testing"
)

type MockAWSKMSSymmetricKey struct {
	internal.AWSKMSSymmetricKey
}

func (symmetricKey *MockAWSKMSSymmetricKey) Encrypt() error {
	salt := "152 random bytes to imitate aws kms encryption method, random words here: witch collapse practice feed shame open despair creek road again ice least it!"
	symmetricKey.EncryptedSymmetricKey = append(symmetricKey.SymmetricKey, salt...)
	return nil
}

func (symmetricKey *MockAWSKMSSymmetricKey) Decrypt() error {
	symmetricKey.SymmetricKey = symmetricKey.EncryptedSymmetricKey[:symmetricKey.SymmetricKeyLen]
	return nil
}

func NewMockSymmetricKey(kmsKeyId string, keyLen int, encryptedKeyLen int) internal.SymmetricKey {
	return &MockAWSKMSSymmetricKey{internal.AWSKMSSymmetricKey{SymmetricKeyLen: keyLen, EncryptedSymmetricKeyLen: encryptedKeyLen, KeyId: kmsKeyId}}
}

type MockAWSKMSCrypter struct {
	internal.AWSKMSCrypter
}

func (crypter *MockAWSKMSCrypter) IsUsed() bool {
	crypter.Configured = true

	crypter.SymmetricKey = NewMockSymmetricKey("AWSKMSKEYID", 32, 184)

	return true
}

func TestAWSKMSCrypterEncryption(t *testing.T) {
	const someSecret = "so very secret thingy"

	crypter := &MockAWSKMSCrypter{}
	crypter.IsUsed()

	buf := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(&ClosingBuffer{buf})
	assert.NoErrorf(t, err, "Encryption error: %v", err)

	encrypt.Write([]byte(someSecret))
	encrypt.Close()

	decrypt, err := crypter.Decrypt(&ClosingBuffer{buf})
	assert.NoErrorf(t, err, "Decryption error: %v", err)

	decryptedBytes, err := ioutil.ReadAll(decrypt)
	assert.NoErrorf(t, err, "Decryption read error: %v", err)

	assert.Equal(t, someSecret, string(decryptedBytes), "Decrypted text not equals open text")
}

func TestAWSKMSCrypterGetType(t *testing.T) {
	crypter := &internal.AWSKMSCrypter{}
	assert.Equal(t, "aws-kms", crypter.GetType(), "Returning crypter type not working")
}
