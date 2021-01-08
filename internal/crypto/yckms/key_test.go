package yckms

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSerializeDeserializeKeyHeader(t *testing.T) {
	buffer := new(bytes.Buffer)

	encryptedKey := make([]byte, 64)
	for i := range encryptedKey {
		encryptedKey[i] = 0xaa
	}

	serializedKey := serializeEncryptedKey(encryptedKey)
	buffer.Write(serializedKey)

	deserializedKey, err := deserializeEncryptedKey(buffer)
	assert.NoErrorf(t, err, "YcKms key deserialization error: %v", err)

	assert.Equal(t, len(encryptedKey), len(deserializedKey), "YcKms deserialized key len is not equal to the original one")

	for i := range encryptedKey {
		assert.Equal(t, encryptedKey[i], deserializedKey[i], "YcKms deserialized key is not equal to the original one in position: %d", i)
	}
}
