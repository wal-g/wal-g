package cached

import (
	"crypto/sha1"
	"fmt"
	"io"
	"sync"

	"github.com/wal-g/wal-g/internal/crypto/envelope"
)

type Item struct {
	Object []byte
}

type CachedEnveloper struct {
	wrapped envelope.EnveloperInterface
	items   map[string]Item
	locker  sync.RWMutex
}

func createKey(s []byte) string { return fmt.Sprintf("%x", sha1.Sum(s)) }

func (enveloper *CachedEnveloper) GetName() string {
	return enveloper.wrapped.GetName()
}

func (enveloper *CachedEnveloper) GetEncryptedKey(r io.Reader) ([]byte, error) {
	return enveloper.wrapped.GetEncryptedKey(r)
}

func (enveloper *CachedEnveloper) DecryptKey(encryptedKey []byte) ([]byte, error) {
	key := createKey(encryptedKey)
	enveloper.locker.RLock()
	item, ok := enveloper.items[key]
	enveloper.locker.RUnlock()
	if ok {
		return item.Object, nil
	}

	decryptedKey, err := enveloper.wrapped.DecryptKey(encryptedKey)
	if err != nil {
		return nil, err
	}
	enveloper.locker.Lock()
	defer enveloper.locker.Unlock()
	enveloper.items[key] = Item{
		Object: decryptedKey,
	}
	return decryptedKey, nil

}

func (enveloper *CachedEnveloper) SerializeEncryptedKey(encryptedKey []byte) []byte {
	return enveloper.wrapped.SerializeEncryptedKey(encryptedKey)
}

func EnveloperWithCache(enveloper envelope.EnveloperInterface) envelope.EnveloperInterface {
	return &CachedEnveloper{
		wrapped: enveloper,
		items:   make(map[string]Item),
	}
}
