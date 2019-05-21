package openpgp

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
	"golang.org/x/crypto/openpgp"
)

var pgpTestPrivateKey string

const PrivateKeyFilePath = "./testdata/pgpTestPrivateKey"

func init() {
	pgpTestPrivateKeyBytes, err := ioutil.ReadFile(PrivateKeyFilePath)
	if err != nil {
		panic(err)
	}
	pgpTestPrivateKey = string(pgpTestPrivateKeyBytes)
}

func CreateCrypter(armedKeyring string) *Crypter {
	ring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armedKeyring))
	if err != nil {
		panic(err)
	}
	crypter := &Crypter{PubKey: ring, SecretKey: ring}
	return crypter
}

func MockArmedCrypter() crypto.Crypter {
	return CreateCrypter(pgpTestPrivateKey)
}

func TestMockCrypter(t *testing.T) {
	MockArmedCrypter()
}

func TestEncryptionCycle(t *testing.T) {
	crypter := MockArmedCrypter()
	const someSecret = "so very secret thingy"

	buf := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(buf)
	assert.NoErrorf(t, err, "Encryption error: %v", err)

	encrypt.Write([]byte(someSecret))
	encrypt.Close()

	decrypt, err := crypter.Decrypt(buf)
	assert.NoErrorf(t, err, "Decryption error: %v", err)

	decryptedBytes, err := ioutil.ReadAll(decrypt)
	assert.NoErrorf(t, err, "Decryption read error: %v", err)

	assert.Equal(t, someSecret, string(decryptedBytes), "Decrypted text not equals open text")
}
