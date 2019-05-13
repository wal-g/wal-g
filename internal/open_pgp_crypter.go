package internal

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal/tracelog"
	"golang.org/x/crypto/openpgp"
)

// OpenPGPCrypter incapsulates specific of cypher method
// Includes keys, infrastructure information etc
// If many encryption methods will be used it worth
// to extract interface
type OpenPGPCrypter struct {
	KeyRingId      string
	IsUseKeyRingId bool

	ArmoredKey      string
	IsUseArmoredKey bool

	ArmoredKeyPath      string
	IsUseArmoredKeyPath bool

	PubKey    openpgp.EntityList
	SecretKey openpgp.EntityList

	mutex sync.RWMutex
}

// NewOpenPGPCrypter created OpenPGPCrypter and configures it.
// In case that OpenPGPCrypter is not armed, it returns nil.
func NewOpenPGPCrypter() Crypter {
	crypter := &OpenPGPCrypter{}
	crypter.configure()
	if !crypter.isArmed() {
		return nil
	}
	return crypter
}

func (crypter *OpenPGPCrypter) isArmed() bool {
	if crypter.IsUseKeyRingId {
		tracelog.WarningLogger.Println(`
You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set GPG key using environment variables WALG_PGP_KEY or WALG_PGP_KEY_PATH.
		`)
	}

	return crypter.IsUseArmoredKey || crypter.IsUseArmoredKeyPath || crypter.IsUseKeyRingId
}

// configure internal initialization
func (crypter *OpenPGPCrypter) configure() {
	// key can be either private (for download) or public (for upload)
	armoredKey, isKeyExist := LookupConfigValue("WALG_PGP_KEY")

	if isKeyExist {
		crypter.ArmoredKey = armoredKey
		crypter.IsUseArmoredKey = true

		return
	}

	// key can be either private (for download) or public (for upload)
	armoredKeyPath, isPathExist := LookupConfigValue("WALG_PGP_KEY_PATH")

	if isPathExist {
		crypter.ArmoredKeyPath = armoredKeyPath
		crypter.IsUseArmoredKeyPath = true

		return
	}

	if crypter.KeyRingId = GetKeyRingId(); crypter.KeyRingId != "" {
		crypter.IsUseKeyRingId = true
	}
}

func (crypter *OpenPGPCrypter) setupPubKey() error {
	crypter.mutex.RLock()
	if crypter.PubKey != nil {
		crypter.mutex.RUnlock()
		return nil
	}
	crypter.mutex.RUnlock()

	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()
	if crypter.PubKey != nil { // already set up
		return nil
	}

	if crypter.IsUseArmoredKey {
		entityList, err := openpgp.ReadArmoredKeyRing(strings.NewReader(crypter.ArmoredKey))

		if err != nil {
			return err
		}

		crypter.PubKey = entityList
	} else if crypter.IsUseArmoredKeyPath {
		entityList, err := ReadPGPKey(crypter.ArmoredKeyPath)

		if err != nil {
			return err
		}

		crypter.PubKey = entityList
	} else {
		// TODO: legacy gpg external use, need to remove in next major version
		armor, err := getPubRingArmor(crypter.KeyRingId)

		if err != nil {
			return err
		}

		entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armor))

		if err != nil {
			return err
		}

		crypter.PubKey = entityList
	}
	return nil
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *OpenPGPCrypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	err := crypter.setupPubKey()
	if err != nil {
		return nil, err
	}

	// We use buffered writer because encryption starts writing header immediately,
	// which can be inappropriate for further usage with blocking writers.
	// E. g. if underlying writer is a pipe, then this thread will be blocked before
	// creation of new thread, reading from this pipe.Writer.
	bufferedWriter := bufio.NewWriter(writer)
	encryptedWriter, err := openpgp.Encrypt(bufferedWriter, crypter.PubKey, nil, nil, nil)

	if err != nil {
		return nil, errors.Wrapf(err, "opengpg encryption error")
	}

	return NewOnCloseFlusher(encryptedWriter, bufferedWriter), nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *OpenPGPCrypter) Decrypt(reader io.Reader) (io.Reader, error) {
	err := crypter.loadSecret()

	if err != nil {
		return nil, err
	}

	md, err := openpgp.ReadMessage(reader, crypter.SecretKey, nil, nil)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return md.UnverifiedBody, nil
}

// load the secret key based on the settings
func (crypter *OpenPGPCrypter) loadSecret() error {
	// check if we actually need to load it
	crypter.mutex.RLock()
	if crypter.SecretKey != nil {
		crypter.mutex.RUnlock()
		return nil
	}
	// unlock needs to be there twice due to different code paths
	crypter.mutex.RUnlock()

	// we need to load, so lock for writing
	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()

	// double check as the key might have been loaded between the RUnlock and Lock
	if crypter.SecretKey != nil {
		return nil
	}

	if crypter.IsUseArmoredKey {
		entityList, err := openpgp.ReadArmoredKeyRing(strings.NewReader(crypter.ArmoredKey))

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	} else if crypter.IsUseArmoredKeyPath {
		entityList, err := ReadPGPKey(crypter.ArmoredKeyPath)

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	} else {
		// TODO: legacy gpg external use, need to remove in next major version
		armor, err := getSecretRingArmor(crypter.KeyRingId)

		if err != nil {
			return errors.WithStack(err)
		}

		entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armor))

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	}

	if passphrase, isExist := LookupConfigValue("WALG_PGP_KEY_PASSPHRASE"); isExist {
		err := DecryptSecretKey(crypter.SecretKey, passphrase)

		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
