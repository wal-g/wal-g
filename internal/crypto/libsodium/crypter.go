package libsodium

// #cgo CFLAGS: -I../../../tmp/libsodium/include
// #cgo LDFLAGS: -L../../../tmp/libsodium/lib -lsodium
// #include <sodium.h>
import "C"

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	chunkSize         = 8192
	libsodiumKeybytes = 32
	minimalKeyLength  = 25
)

// libsodium should always be initialized
func init() {
	C.sodium_init()
}

// Crypter is libsodium Crypter implementation
type Crypter struct {
	key []byte

	KeyInline    string
	KeyPath      string
	KeyTransform string

	mutex sync.RWMutex
}

func (crypter *Crypter) Name() string {
	return "Libsodium"
}

// CrypterFromKey creates Crypter from key
func CrypterFromKey(key string, keyTransform string) crypto.Crypter {
	return &Crypter{KeyInline: key, KeyTransform: keyTransform}
}

// CrypterFromKeyPath creates Crypter from key path
func CrypterFromKeyPath(path string, keyTransform string) crypto.Crypter {
	return &Crypter{KeyPath: path, KeyTransform: keyTransform}
}

func (crypter *Crypter) setup() (err error) {
	crypter.mutex.RLock()
	if crypter.key != nil {
		crypter.mutex.RUnlock()
		return nil
	}
	crypter.mutex.RUnlock()

	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()

	if crypter.key != nil {
		return nil
	}

	if crypter.KeyInline == "" && crypter.KeyPath == "" {
		return errors.New("libsodium Crypter: must have a key or key path")
	}

	keyString := crypter.KeyInline
	if keyString == "" {
		// read from file
		keyFileContents, err := os.ReadFile(crypter.KeyPath)
		if err != nil {
			return fmt.Errorf("libsodium Crypter: unable to read key from file: %v", err)
		}

		keyString = strings.TrimSpace(string(keyFileContents))
	}

	key, err := keyTransform(keyString, crypter.KeyTransform, libsodiumKeybytes)
	if err != nil {
		return fmt.Errorf("libsodium Crypter: during key transform: %v", err)
	}

	crypter.key = key
	return nil
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *Crypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	if err := crypter.setup(); err != nil {
		return nil, err
	}

	return NewWriter(writer, crypter.key), nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *Crypter) Decrypt(reader io.Reader) (io.Reader, error) {
	if err := crypter.setup(); err != nil {
		return nil, err
	}

	return NewReader(reader, crypter.key), nil
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
