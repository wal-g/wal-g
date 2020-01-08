// +build libsodium

package libsodium

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	keyPath = "./testdata/testKey"
	testKey = "TEST_LIBSODIUM_KEY"
)

func MockCrypterFromKey() *Crypter {
	return CrypterFromKey(testKey).(*Crypter)
}

func MockCrypterFromKeyPath() *Crypter {
	return CrypterFromKeyPath(keyPath).(*Crypter)
}

func TestMockCrypterFromKey(t *testing.T) {
	assert.NoError(t, MockCrypterFromKey().setup(), "setup Crypter from key error")
}

func TestMockCrypterFromKeyPath(t *testing.T) {
	assert.NoError(t, MockCrypterFromKeyPath().setup(), "setup Crypter from key path error")
}

func TestMockCrypterFromKey_ShouldReturnErrorOnEmptyKey(t *testing.T) {
	assert.Error(t, CrypterFromKey("").(*Crypter).setup(), "no error on empty key")
}

func TestMockCrypterFromKeyPath_ShouldReturnErrorOnNonExistentFile(t *testing.T) {
	assert.Error(t, CrypterFromKeyPath("").(*Crypter).setup(), "no error on non-existent key path")
}

func EncryptionCycle(t *testing.T, crypter crypto.Crypter) {
	secret := strings.Repeat(" so very secret thing ", 1000)

	buffer := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(buffer)
	assert.NoErrorf(t, err, "encryption error: %v", err)

	encrypt.Write([]byte(secret))
	encrypt.Close()

	decrypt, err := crypter.Decrypt(buffer)
	assert.NoErrorf(t, err, "decryption error: %v", err)

	decrypted, err := ioutil.ReadAll(decrypt)
	assert.NoErrorf(t, err, "decryption read error: %v", err)

	assert.Equal(t, secret, string(decrypted), "decrypted text not equals to open text")
}

func TestEncryptionCycleFromKey(t *testing.T) {
	EncryptionCycle(t, MockCrypterFromKey())
}

func TestEncryptionCycleFromKeyPath(t *testing.T) {
	EncryptionCycle(t, MockCrypterFromKeyPath())
}
