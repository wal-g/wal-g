package libsodium

// #cgo CFLAGS: -I../../../tmp/libsodium/include
// #cgo LDFLAGS: -L../../../tmp/libsodium/lib -lsodium
// #include <sodium.h>
import "C"

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	chunkSize = 8192
	minimalKeyLength = 25
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

func (crypter *Crypter) Name() string {
	return "Libsodium"
}

// CrypterFromKey creates Crypter from key
func CrypterFromKey(key string) (crypto.Crypter, error) {
	if len(key) < minimalKeyLength {
		return nil, newErrShortKey(len(key))
	}

	return &Crypter{Key: key}, nil
}

// CrypterFromKeyPath creates Crypter from key path
func CrypterFromKeyPath(path string) crypto.Crypter {
	return &Crypter{KeyPath: path}
}

func (crypter *Crypter) setup() (err error) {
	crypter.mutex.RLock()

	if crypter.Key == "" && crypter.KeyPath == "" {
		crypter.mutex.RUnlock()

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

	return NewWriter(writer, []byte(crypter.Key)), nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *Crypter) Decrypt(reader io.Reader) (io.Reader, error) {
	if err := crypter.setup(); err != nil {
		return nil, err
	}

	return NewReader(reader, []byte(crypter.Key)), nil
}

var _ error = &ErrShortKey{}

type ErrShortKey struct {
	keyLength int
}

func (e ErrShortKey) Error() string {
	return fmt.Sprintf("key length must not be less than %v, got %v", minimalKeyLength, e.keyLength)
}

func newErrShortKey(keyLength int) *ErrShortKey {
	return &ErrShortKey{
		keyLength: keyLength,
	}
}
