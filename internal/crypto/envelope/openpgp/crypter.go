package openpgp

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/pkg/errors"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/envelope"
	"github.com/wal-g/wal-g/internal/ioextensions"
)

const (
	maxHeaderLenAllowed int = 4096 * 2
)

// Crypter incapsulates specific of cypher method
// Includes keys, infrastructure information etc
type Crypter struct {
	enveloper    envelope.Enveloper
	encryptedKey []byte

	ArmoredKey      string
	IsUseArmoredKey bool

	ArmoredKeyPath      string
	IsUseArmoredKeyPath bool

	mutex sync.RWMutex
}

func (crypter *Crypter) Name() string {
	parts := []string{"Enveloped", crypter.enveloper.GetName(), "Opengpg", "Crypter"}
	return strings.Join(parts, "/")
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *Crypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	err := crypter.setupEncryptedKey()
	if err != nil {
		return nil, err
	}

	// need write header at first, with length less than maxHeaderLenAllowed
	bufferedWriter := bufio.NewWriterSize(writer, maxHeaderLenAllowed)
	header := crypter.enveloper.SerializeEncryptedKey(crypter.encryptedKey)

	if len(header) > maxHeaderLenAllowed {
		return nil, errors.New("opengpg: invalid size of the encrypted key")
	}
	_, err = bufferedWriter.Write(header)
	if err != nil {
		tracelog.ErrorLogger.Printf("Can't write encryption key to buffer: %v", err)
		return nil, err
	}
	key, err := crypter.enveloper.DecryptKey(crypter.encryptedKey)
	pubKey, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(key))

	encryptedWriter, err := openpgp.Encrypt(bufferedWriter, pubKey, nil, nil, nil)

	if err != nil {
		return nil, errors.Wrapf(err, "opengpg encryption error")
	}

	return ioextensions.NewOnCloseFlusher(encryptedWriter, bufferedWriter), nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *Crypter) Decrypt(reader io.Reader) (io.Reader, error) {

	// need read header at first, with length less than maxHeaderLenAllowed
	bufferedReader := bufio.NewReaderSize(reader, maxHeaderLenAllowed)
	encryptedKey, err := crypter.enveloper.GetEncryptedKey(bufferedReader)
	if err != nil {
		return nil, err
	}
	key, err := crypter.enveloper.DecryptKey(encryptedKey)
	secretKey, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(key))

	md, err := openpgp.ReadMessage(bufferedReader, secretKey, nil, nil)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return md.UnverifiedBody, nil
}

func (crypter *Crypter) setupEncryptedKey() error {
	crypter.mutex.RLock()
	if crypter.encryptedKey != nil {
		crypter.mutex.RUnlock()
		return nil
	}
	crypter.mutex.RUnlock()

	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()
	if crypter.encryptedKey != nil {
		return nil
	}

	switch {
	case crypter.IsUseArmoredKey:
		encryptedKey, err := hex.DecodeString(strings.TrimSuffix(crypter.ArmoredKey, "\n"))
		if err != nil {
			return err
		}
		crypter.encryptedKey = encryptedKey
	case crypter.IsUseArmoredKeyPath:
		encryptedKey, err := os.ReadFile(crypter.ArmoredKeyPath)
		if err != nil {
			return err
		}
		crypter.encryptedKey = encryptedKey
	}
	return nil
}

// CrypterFromKey creates Crypter from encrypted armored key.
func CrypterFromKey(armoredKey string, enveloper envelope.Enveloper) crypto.Crypter {
	return &Crypter{
		ArmoredKey:      armoredKey,
		IsUseArmoredKey: true,
		enveloper:       enveloper,
	}
}

// CrypterFromKeyPath creates Crypter from encrypted armored key path.
func CrypterFromKeyPath(armoredKeyPath string, enveloper envelope.Enveloper) crypto.Crypter {
	return &Crypter{
		ArmoredKeyPath:      armoredKeyPath,
		IsUseArmoredKeyPath: true,
		enveloper:           enveloper,
	}
}
