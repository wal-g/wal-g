package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"io/ioutil"
	"testing"
)

type MockAWSKMSCrypter struct {
	internal.AWSKMSCrypter
}

func (crypter *MockAWSKMSCrypter) IsArmed() bool {
	return true
}

func (crypter *MockAWSKMSCrypter) EncryptSymmetricKey() error {
	salt := "152 random bytes to imitate aws kms encryption method, random words here: witch collapse practice feed shame open despair creek road again ice least it!"
	crypter.EncryptedSymmetricKey = append(crypter.SymmetricKey, salt...)
	return nil
}

func (crypter *MockAWSKMSCrypter) DecryptSymmetricKey() error {
	crypter.SymmetricKey = crypter.EncryptedSymmetricKey[:crypter.SymmetricKeyLen]
	return nil
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
