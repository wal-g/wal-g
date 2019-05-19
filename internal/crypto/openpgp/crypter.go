package openpgp

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/internal/utils"
	"golang.org/x/crypto/openpgp"
)

// Crypter incapsulates specific of cypher method
// Includes keys, infrastructure information etc
// If many encryption methods will be used it worth
// to extract interface
type Crypter struct {
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

// NewCrypter created Crypter and configures it.
// In case that Crypter is not armed, it returns nil.
func NewCrypter() crypto.Crypter {
	crypter := &Crypter{}
	crypter.configure()
	if !crypter.isArmed() {
		return nil
	}
	return crypter
}

func (crypter *Crypter) isArmed() bool {
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
func (crypter *Crypter) configure() {
	// key can be either private (for download) or public (for upload)
	armoredKey, isKeyExist := config.LookupConfigValue("WALG_PGP_KEY")

	if isKeyExist {
		crypter.ArmoredKey = armoredKey
		crypter.IsUseArmoredKey = true

		return
	}

	// key can be either private (for download) or public (for upload)
	armoredKeyPath, isPathExist := config.LookupConfigValue("WALG_PGP_KEY_PATH")

	if isPathExist {
		crypter.ArmoredKeyPath = armoredKeyPath
		crypter.IsUseArmoredKeyPath = true

		return
	}

	if crypter.KeyRingId = crypto.GetKeyRingId(); crypter.KeyRingId != "" {
		crypter.IsUseKeyRingId = true
	}
}

func (crypter *Crypter) setupPubKey() error {
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
		entityList, err := readPGPKey(crypter.ArmoredKeyPath)

		if err != nil {
			return err
		}

		crypter.PubKey = entityList
	} else {
		// TODO: legacy gpg external use, need to remove in next major version
		armor, err := crypto.GetPubRingArmor(crypter.KeyRingId)

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
func (crypter *Crypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	err := crypter.setupPubKey()
	if err != nil {
		return nil, err
	}

	// We use buffered writer because encryption starts wrdepiting header immediately,
	// which can be inappropriate for further usage with blocking writers.
	// E. g. if underlying writer is a pipe, then this thread will be blocked before
	// creation of new thread, reading from this pipe.Writer.
	bufferedWriter := bufio.NewWriter(writer)
	encryptedWriter, err := openpgp.Encrypt(bufferedWriter, crypter.PubKey, nil, nil, nil)

	if err != nil {
		return nil, errors.Wrapf(err, "opengpg encryption error")
	}

	return utils.NewOnCloseFlusher(encryptedWriter, bufferedWriter), nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *Crypter) Decrypt(reader io.Reader) (io.Reader, error) {
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
func (crypter *Crypter) loadSecret() error {
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
		entityList, err := readPGPKey(crypter.ArmoredKeyPath)

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	} else {
		// TODO: legacy gpg external use, need to remove in next major version
		armor, err := crypto.GetSecretRingArmor(crypter.KeyRingId)

		if err != nil {
			return errors.WithStack(err)
		}

		entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armor))

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	}

	if passphrase, isExist := config.LookupConfigValue("WALG_PGP_KEY_PASSPHRASE"); isExist {
		err := decryptSecretKey(crypter.SecretKey, passphrase)

		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
