package yckms

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wal-g/wal-g/internal/crypto/envelope"
)

func TestSerializeDeserializeKeyHeader(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := envelope.NewEncryptedKey("example", []byte("encrypted key"))
	serializedKey := serializeEncryptedKey(expected)
	buffer.Write(serializedKey)

	encryptedKey, err := readEncryptedKey(buffer)
	assert.NoErrorf(t, err, "YcKms envelope key deserialization error: %v", err)

	assert.Equal(t, expected.ID, encryptedKey.ID, "YcKms deserialized envelope key len is not equal to the original one")
	assert.Equal(t, len(expected.Data), len(encryptedKey.Data), "YcKms deserialized envelope key len is not equal to the original one")

	for i := range expected.Data {
		assert.Equal(t, expected.Data[i], encryptedKey.Data[i], "YcKms deserialized envelope key is not equal to the original one in position: %d", i)
	}
}

func TestHugeKey(t *testing.T) {
	reader, writer := io.Pipe()

	expected := envelope.NewEncryptedKey("example", []byte(strings.Repeat("awesomekey", 512)))
	serializedKey := serializeEncryptedKey(expected)
	go func() {
		defer writer.Close()
		writer.Write(serializedKey)
	}()
	breader := bufio.NewReaderSize(reader, 16)

	encryptedKey, err := readEncryptedKey(breader)
	assert.NoErrorf(t, err, "YcKms envelope key deserialization error: %v", err)

	assert.Equal(t, expected.ID, encryptedKey.ID, "YcKms deserialized envelope key len is not equal to the original one")
	assert.Equal(t, len(expected.Data), len(encryptedKey.Data), "YcKms deserialized envelope key len is not equal to the original one")

	for i := range expected.Data {
		assert.Equal(t, expected.Data[i], encryptedKey.Data[i], "YcKms deserialized envelope key is not equal to the original one in position: %d", i)
	}
}
