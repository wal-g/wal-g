package openpgp

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/envelope"
)

const (
	PrivateKeyFilePath             = "./testdata/pgpTestPrivateKey"
	PrivateEncryptedKeyFilePath    = "./testdata/pgpTestEncryptedPrivateKey"
	PrivateEncryptedKeyEnvFilePath = "./testdata/pgpTestEncryptedPrivateKeyEnv"
)

type mockedEnveloper struct {
	key []byte
}

func (enveloper *mockedEnveloper) GetName() string {
	return "mocked"
}

func (enveloper *mockedEnveloper) GetEncryptedKey(r io.Reader) ([]byte, error) {
	return []byte(""), nil
}

func (enveloper *mockedEnveloper) DecryptKey(encryptedKey []byte) ([]byte, error) {
	return enveloper.key, nil
}

func (enveloper *mockedEnveloper) SerializeEncryptedKey(encryptedKey []byte) []byte {
	return []byte("")
}

func MockedEnveloper() envelope.EnveloperInterface {
	key, err := os.ReadFile(PrivateKeyFilePath)
	if err != nil {
		panic(err)
	}
	return &mockedEnveloper{
		key: key,
	}
}

func MockArmedCrypterFromEnv(enveloper envelope.EnveloperInterface) crypto.Crypter {
	rawEnv, err := os.ReadFile(PrivateEncryptedKeyEnvFilePath)
	if err != nil {
		panic(err)
	}
	env := string(rawEnv)
	return CrypterFromKey(string(env), enveloper)
}

func MockArmedCrypterFromKeyPath(enveloper envelope.EnveloperInterface) crypto.Crypter {
	return CrypterFromKeyPath(PrivateEncryptedKeyFilePath, enveloper)
}

func TestMockCrypterFromEnv(t *testing.T) {
	enveloper := MockedEnveloper()
	MockArmedCrypterFromEnv(enveloper)
}

func TestMockCrypterFromKeyPath(t *testing.T) {
	enveloper := MockedEnveloper()
	MockArmedCrypterFromKeyPath(enveloper)
}

func EncryptionCycle(t *testing.T, crypter crypto.Crypter) {
	const someSecret = "so very secret thingy"

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

func TestEncryptionCycleFromEnv(t *testing.T) {
	enveloper := MockedEnveloper()
	EncryptionCycle(t, MockArmedCrypterFromEnv(enveloper))
}

func TestEncryptionCycleFromKeyPath(t *testing.T) {
	enveloper := MockedEnveloper()
	EncryptionCycle(t, MockArmedCrypterFromKeyPath(enveloper))
}
