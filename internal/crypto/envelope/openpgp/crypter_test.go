package openpgp

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/envelope"
	"github.com/wal-g/wal-g/internal/crypto/envelope/mocks"
)

const (
	PrivateKeyFilePath             = "./testdata/pgpTestPrivateKey"
	PrivateEncryptedKeyFilePath    = "./testdata/pgpTestEncryptedPrivateKey"
	PrivateEncryptedKeyEnvFilePath = "./testdata/pgpTestEncryptedPrivateKeyEnv"
)

func MockedEnveloper(t *testing.T) *mocks.Enveloper {
	key, err := os.ReadFile(PrivateKeyFilePath)
	if err != nil {
		panic(err)
	}
	enveloper := mocks.NewEnveloper(t)
	enveloper.EXPECT().Name().Return("mocked").Maybe()
	enveloper.EXPECT().ReadEncryptedKey(mock.Anything).Return([]byte(""), nil).Maybe()
	enveloper.EXPECT().DecryptKey(mock.Anything).Return(key, nil).Maybe()
	enveloper.EXPECT().SerializeEncryptedKey(mock.Anything, mock.Anything).Return([]byte("")).Maybe()
	return enveloper
}

func MockArmedCrypterFromEnv(enveloper envelope.Enveloper) crypto.Crypter {
	rawEnv, err := os.ReadFile(PrivateEncryptedKeyEnvFilePath)
	if err != nil {
		panic(err)
	}
	env := string(rawEnv)
	return CrypterFromKey(env, enveloper)
}

func MockArmedCrypterFromKeyPath(enveloper envelope.Enveloper) crypto.Crypter {
	return CrypterFromKeyPath(PrivateEncryptedKeyFilePath, enveloper)
}

func TestMockCrypterFromEnv(t *testing.T) {
	enveloper := MockedEnveloper(t)
	MockArmedCrypterFromEnv(enveloper)
}

func TestMockCrypterFromKeyPath(t *testing.T) {
	enveloper := MockedEnveloper(t)
	MockArmedCrypterFromKeyPath(enveloper)
}

func EncryptionCycle(t *testing.T, crypter crypto.Crypter) {
	const someSecret = "so very secret thing"

	buf := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(buf)
	assert.NoErrorf(t, err, "Encryption error: %v", err)

	_, err = encrypt.Write([]byte(someSecret))
	assert.NoError(t, err)
	err = encrypt.Close()
	assert.NoError(t, err)

	decrypt, err := crypter.Decrypt(buf)
	assert.NoErrorf(t, err, "Decryption error: %v", err)

	decryptedBytes, err := io.ReadAll(decrypt)
	assert.NoErrorf(t, err, "Decryption read error: %v", err)

	assert.Equal(t, someSecret, string(decryptedBytes), "Decrypted text not equals open text")
}

func TestEncryptionCycleFromEnv(t *testing.T) {
	enveloper := MockedEnveloper(t)
	EncryptionCycle(t, MockArmedCrypterFromEnv(enveloper))
}

func TestEncryptionCycleFromKeyPath(t *testing.T) {
	enveloper := MockedEnveloper(t)
	EncryptionCycle(t, MockArmedCrypterFromKeyPath(enveloper))
}

func TestEncodeKeyID(t *testing.T) {
	key, err := os.ReadFile(PrivateKeyFilePath)
	assert.NoError(t, err)
	entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(key))
	assert.NoError(t, err)
	keyID, err := encodeKeyID(entityList)
	assert.NoError(t, err)
	assert.Equal(t, "3BE0C94F8BDCA96B", keyID, "Key id is mismatch")
}

func TestEncodeEmptyKeyID(t *testing.T) {
	var emptyKey []*openpgp.Entity
	keyID, err := encodeKeyID(emptyKey)
	assert.NoError(t, err)
	assert.Equal(t, "", keyID, "Key id is mismatch")
}
