package test

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
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

func MockArmedCrypter() internal.Crypter {
	return createCrypter(pgpTestPrivateKey)
}

func createCrypter(armedKeyring string) *internal.OpenPGPCrypter {
	ring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armedKeyring))
	if err != nil {
		panic(err)
	}
	crypter := &internal.OpenPGPCrypter{Configured: true, PubKey: ring, SecretKey: ring}
	return crypter
}

func MockDisarmedCrypter() internal.Crypter {
	return &MockCrypter{}
}

type MockCrypter struct {
}

func (crypter *MockCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	return writer, nil
}

func (crypter *MockCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	return reader, nil
}

func (crypter *MockCrypter) IsUsed() bool {
	return true
}

func TestMockCrypter(t *testing.T) {
	MockArmedCrypter()
	MockDisarmedCrypter()
}

type ClosingBuffer struct {
	*bytes.Buffer
}

func (cb *ClosingBuffer) Close() (err error) {
	return nil
}

func TestEncryptionCycle(t *testing.T) {
	crypter := MockArmedCrypter()
	const someSecret = "so very secret thingy"

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
