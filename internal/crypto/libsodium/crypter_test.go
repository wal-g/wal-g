//go:build libsodium
// +build libsodium

package libsodium

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	keyPath = "./testdata/testKey"
	testKey = "TEST_LIBSODIUM_KEY_______"
)

func MockCrypterFromKey() (*Crypter, error) {
	cr, err := CrypterFromKey(testKey)
	if err != nil {
		return nil, err
	}
	return cr.(*Crypter), err
}

func MockCrypterFromKeyPath() *Crypter {
	return CrypterFromKeyPath(keyPath).(*Crypter)
}

func TestMockCrypterFromKey(t *testing.T) {
	cr, err := MockCrypterFromKey()
	require.NoError(t, err)
	assert.NoError(t, cr.setup(), "setup Crypter from key error")
}

func TestMockCrypterFromKeyPath(t *testing.T) {
	assert.NoError(t, MockCrypterFromKeyPath().setup(), "setup Crypter from key path error")
}

func TestMockCrypterFromKey_ShouldReturnErrorOnEmptyKey(t *testing.T) {
	tests := map[string]struct {
		key string
	}{
		"empty": {key: ""},
		"short": {key: "short_key"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := CrypterFromKey(test.key)
			assert.Error(t, err, "no error on short key")
		})
	}
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
	cr, err := MockCrypterFromKey()
	require.NoError(t, err)
	EncryptionCycle(t, cr)
}

func TestEncryptionCycleFromKeyPath(t *testing.T) {
	EncryptionCycle(t, MockCrypterFromKeyPath())
}
