//go:build libsodium
// +build libsodium

package libsodium

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	keyPath = "./testdata/testKey"
	testKey = "TEST_LIBSODIUM_KEY_______"
)

func MockCrypterFromKey() *Crypter {
	if len(testKey) < 25 {
		panic(fmt.Errorf("libsodium key length must not be less than 25, got %v", len(testKey)))
	}

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
	reader, writer := io.Pipe()

	encrypt, err := crypter.Encrypt(writer)
	assert.NoErrorf(t, err, "encryption error: %v", err)

	decrypt, err := crypter.Decrypt(reader)
	assert.NoErrorf(t, err, "decryption error: %v", err)

	go func() {
		encrypt.Write([]byte(secret))
		encrypt.Close()
	}()

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
