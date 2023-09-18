package cached

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/wal-g/wal-g/internal/crypto/envelope"
	"github.com/wal-g/wal-g/internal/crypto/envelope/mocks"
)

func TestDecryptKey(t *testing.T) {
	encryptedKey := envelope.NewEncryptedKey("example", []byte("encrypted key"))
	expected := []byte("decrypted key")

	t.Run("success", func(t *testing.T) {
		mockEnveloper := mocks.NewEnveloper(t)
		cached := EnveloperWithCache(mockEnveloper, time.Minute)
		mockEnveloper.EXPECT().DecryptKey(mock.Anything).Return(expected, nil).Once()
		key, err := cached.DecryptKey(encryptedKey)
		assert.NoError(t, err)
		assert.Len(t, key, len(expected))
		for i := range expected {
			assert.Equal(t, expected[i], key[i], "Decrypted key is not equal to the expected in position: %d", i)
		}
	})

	t.Run("cached", func(t *testing.T) {
		mockEnveloper := mocks.NewEnveloper(t)
		cached := EnveloperWithCache(mockEnveloper, time.Minute)
		mockEnveloper.EXPECT().DecryptKey(mock.Anything).Return(expected, nil).Once()
		_, err := cached.DecryptKey(encryptedKey)
		assert.NoError(t, err)

		key, err := cached.DecryptKey(encryptedKey)
		assert.NoError(t, err)

		assert.Len(t, key, len(expected))
		for i := range expected {
			assert.Equal(t, expected[i], key[i], "Decrypted key is not equal to the expected in position: %d", i)
		}
	})

	t.Run("staled cache", func(t *testing.T) {
		mockEnveloper := mocks.NewEnveloper(t)
		cached := EnveloperWithCache(mockEnveloper, time.Millisecond)

		mockEnveloper.EXPECT().DecryptKey(mock.Anything).Return(expected, nil).Once()
		mockEnveloper.EXPECT().DecryptKey(mock.Anything).Return(nil, errors.New("unexpected")).Once()

		_, err := cached.DecryptKey(encryptedKey)
		assert.NoError(t, err)
		time.Sleep(2 * time.Millisecond)
		key, err := cached.DecryptKey(encryptedKey)
		assert.NoError(t, err)

		assert.Len(t, key, len(expected))
		for i := range expected {
			assert.Equal(t, expected[i], key[i], "Decrypted key is not equal to the expected in position: %d", i)
		}
	})

	t.Run("permanent cache", func(t *testing.T) {
		mockEnveloper := mocks.NewEnveloper(t)
		cached := EnveloperWithCache(mockEnveloper, 0)

		mockEnveloper.EXPECT().DecryptKey(mock.Anything).Return(expected, nil).Once()
		mockEnveloper.EXPECT().DecryptKey(mock.Anything).Return([]byte("not an expected"), nil).Times(0)

		_, err := cached.DecryptKey(encryptedKey)
		assert.NoError(t, err)
		key, err := cached.DecryptKey(encryptedKey)
		assert.NoError(t, err)

		assert.Len(t, key, len(expected))
		for i := range expected {
			assert.Equal(t, expected[i], key[i], "Decrypted key is not equal to the expected in position: %d", i)
		}
	})
}
