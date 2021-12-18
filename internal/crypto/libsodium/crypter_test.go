//go:build libsodium
// +build libsodium

package libsodium

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/crypto"
)

func TestMockCrypterFromKey_ShouldReturnErrorOnEmptyKey(t *testing.T) {
	tests := map[string]struct {
		key string
	}{
		"empty": {key: ""},
		"short": {key: "short_key"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := CrypterFromKey(test.key, KeyTransformNone).(*Crypter).setup()
			assert.Error(t, err, "no error on short key")
		})
	}
}

func TestMockCrypterFromKeyPath_ShouldReturnErrorOnNonExistentFile(t *testing.T) {
	assert.Error(t, CrypterFromKeyPath("", KeyTransformNone).(*Crypter).setup(), "no error on non-existent key path")
}

func TestMockCrypterFromKeyPath_ShouldErrorIfTransformFails(t *testing.T) {
	type TestCase struct {
		key       string
		transform string
	}

	testcases := []TestCase{
		// valid hex, invalid length
		{key: "2e4af6d03c7f73f4a80b0594dee2b4bcd11300bafb8a", transform: KeyTransformHex},
		{key: "invalid hex", transform: KeyTransformHex},
		{key: "invalid base64", transform: KeyTransformBase64},
		// valid base64, invalid length
		{key: "DBXYo+QaYKCLSNad+m27jl2UHtW4Htm9pStJv1ujjKPB2N5fmitOFw==", transform: KeyTransformBase64},
	}

	for _, tc := range testcases {
		assert.Error(t, CrypterFromKey(tc.key, tc.transform).(*Crypter).setup(), "no error on invalid encoding")
	}
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
	type TestCase struct {
		keyInline    string
		keyTransform string
	}

	var testcases = []TestCase{
		{keyInline: "TEST_LIBSODIUM_KEY_______", keyTransform: KeyTransformNone},
		{keyInline: "4c0829fdfe7ae1987918edc585b1a90556d901eaea963c7625bb5734576dfb59", keyTransform: KeyTransformHex},
		{keyInline: "jv81yb3v3gNePrY0JmJ4q2j2NrqcM7tDYSHFoZ0tTIw=", keyTransform: KeyTransformBase64},
	}

	for _, tc := range testcases {
		crypter := CrypterFromKey(tc.keyInline, tc.keyTransform).(*Crypter)
		EncryptionCycle(t, crypter)
	}
}

func TestEncryptionCycleFromKeyPath(t *testing.T) {
	type TestCase struct {
		keyPath      string
		keyTransform string
	}

	var testcases = []TestCase{
		{keyPath: "./testdata/testKey", keyTransform: KeyTransformNone},
		{keyPath: "./testdata/testKeyHex", keyTransform: KeyTransformHex},
		{keyPath: "./testdata/testKeyB64", keyTransform: KeyTransformBase64},
	}

	for _, tc := range testcases {
		crypter := CrypterFromKeyPath(tc.keyPath, tc.keyTransform).(*Crypter)
		EncryptionCycle(t, crypter)
	}
}
