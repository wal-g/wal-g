package openpgp

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
)

var pgpTestPrivateKey string

const (
	PrivateKeyFilePath    = "./testdata/pgpTestPrivateKey"
	PrivateKeyEnvFilePath = "./testdata/pgpTestPrivateKeyEnv"
)

func noPassphrase() (string, bool) {
	return "", false
}

func MockArmedCrypterFromEnv() crypto.Crypter {
	pgpTestPrivateKeyBytes, err := os.ReadFile(PrivateKeyEnvFilePath)
	if err != nil {
		panic(err)
	}
	pgpTestPrivateKey = string(pgpTestPrivateKeyBytes)

	return CrypterFromKey(pgpTestPrivateKey, noPassphrase)
}

func MockArmedCrypterFromKeyPath() crypto.Crypter {
	return CrypterFromKeyPath(PrivateKeyFilePath, noPassphrase)
}

func TestMockCrypterFromEnv(t *testing.T) {
	MockArmedCrypterFromEnv()
}

func TestMockCrypterFromKeyPath(t *testing.T) {
	MockArmedCrypterFromKeyPath()
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
	EncryptionCycle(t, MockArmedCrypterFromEnv())
}

func TestEncryptionCycleFromKeyPath(t *testing.T) {
	EncryptionCycle(t, MockArmedCrypterFromKeyPath())
}
