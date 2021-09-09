package libsodium

// #cgo CFLAGS: -I../../../tmp/libsodium/include
// #cgo LDFLAGS: -L../../../tmp/libsodium/lib -lsodium
// #include <sodium.h>
import "C"

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
)

const (
	chunkSize         = 8192
	libsodiumKeybytes = 32
)

// libsodium should always be initialised
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

	if crypter.KeyInline == "" && crypter.KeyPath == "" {
		crypter.mutex.RUnlock()

		return errors.New("libsodium Crypter must have a key or key path")
	}

	if crypter.KeyInline != "" && crypter.KeyPath != "" {
		tracelog.WarningLogger.Println("libsodium Crypter: both key and key path are set, key will have precedence. Set only one of the two options to resolve this warning.")
	}

	if len(crypter.key) == libsodiumKeybytes {
		crypter.mutex.RUnlock()

		return
	}

	crypter.mutex.RUnlock()

	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()

	keyString := crypter.KeyInline
	if keyString == "" {
		// read from file
		var keyFileContents []byte
		keyFileContents, err = ioutil.ReadFile(crypter.KeyPath)
		if err != nil {
			return
		}

		keyString = strings.TrimSpace(string(keyFileContents))
	}

	var rawKey []byte
	if crypter.KeyTransform == "base64" {
		rawKey, err = base64.StdEncoding.DecodeString(keyString)
		if err != nil {
			return fmt.Errorf("libsodium Crypter: while base64 decoding key: %v", err)
		}
	} else if crypter.KeyTransform == "hex" {
		rawKey, err = hex.DecodeString(keyString)
		if err != nil {
			return fmt.Errorf("libsodium Crypter: while hex decoding key: %v", err)
		}
	} else if crypter.KeyTransform == "rpad-zero" {
		// for backward compatibility, to silence the short-key warning
		if len(keyString) < 32 {
			rawKey = make([]byte, 32)
			copy(rawKey, []byte(keyString))
		}
	} else if crypter.KeyTransform == "" {
		if len(keyString) < libsodiumKeybytes {
			// Very short keys may not be able to decrypt backups properly due to out-of-bounds read in previous versions.
			tracelog.WarningLogger.Println("libsodium keys are 32 byte, your key will be padded with zero-bytes to the right. Very short keys used in older versions may not work during backup decryption. Please verify that you can successfully decrypt your backups. You can set WALG_LIBSODIUM_KEY_TRANSFORM to 'rpad-zero' to resolve this warning.")
			rawKey = make([]byte, 32)
			copy(rawKey, []byte(keyString))
		} else if len(keyString) > libsodiumKeybytes {
			// Long keys work, but may give the wrong impression that they are more secure. Previous versions offered no guidance on how to choose a secure key, we provide a warning
			// to inform the user that keys are fixed-size.
			rawKey = []byte(keyString[:32])
			tracelog.WarningLogger.Println("libsodium keys are 32 byte, your key will be truncated to 32 bytes (truncate your key to 32 bytes to resolve this warning).")
		} else {
			rawKey = []byte(keyString)
		}
	} else {
		return errors.New("libsodium Crypter: unknown key transform, must be base64, hex, rpad-zero or empty")
	}

	if len(rawKey) != libsodiumKeybytes {
		return fmt.Errorf("libsodium Crypter: key size must be exactly 32 bytes after transform (got %d bytes)", len(rawKey))
	}

	crypter.key = rawKey

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
