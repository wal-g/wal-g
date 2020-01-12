package libsodium

// #cgo CFLAGS: -I../../../tmp/libsodium/include
// #cgo LDFLAGS: -L../../../tmp/libsodium/lib -lsodium
// #include <sodium.h>
import "C"

import (
	"io"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	chunkSize = 8192
)

// libsodium should always be initialised
func init() {
	C.sodium_init()
}

// Crypter is libsodium Crypter implementation
type Crypter struct {
	Key     string
	KeyPath string

	mutex sync.RWMutex
}

// CrypterFromKey creates Crypter from key
func CrypterFromKey(key string) crypto.Crypter {
	return &Crypter{Key: key}
}

// CrypterFromKeyPath creates Crypter from key path
func CrypterFromKeyPath(path string) crypto.Crypter {
	return &Crypter{KeyPath: path}
}

func (crypter *Crypter) setup() (err error) {
	crypter.mutex.RLock()

	if crypter.Key == "" && crypter.KeyPath == "" {
		return errors.New("libsodium Crypter must have a key or key path")
	}

	if crypter.Key != "" {
		crypter.mutex.RUnlock()

		return
	}

	crypter.mutex.RUnlock()

	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()

	if crypter.Key != "" {
		return
	}

	key, err := ioutil.ReadFile(crypter.KeyPath)

	if err != nil {
		return
	}

	crypter.Key = strings.TrimSpace(string(key))

	return nil
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *Crypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	if err := crypter.setup(); err != nil {
		return nil, err
	}

	return NewWriter(writer, []byte(crypter.Key))
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *Crypter) Decrypt(reader io.Reader) (io.Reader, error) {
	if err := crypter.setup(); err != nil {
		return nil, err
	}

	return NewReader(reader, []byte(crypter.Key))
}
