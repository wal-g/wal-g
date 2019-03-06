package internal

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/wal-g/wal-g/internal/tracelog"
	"golang.org/x/crypto/openpgp"
	"io"
	"strings"
)

// CrypterUseMischiefError happens when crypter is used before initialization
type CrypterUseMischiefError struct {
	error
}

func NewCrypterUseMischiefError() CrypterUseMischiefError {
	return CrypterUseMischiefError{errors.New("crypter is not checked before use")}
}

func (err CrypterUseMischiefError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// OpenPGPCrypter incapsulates specific of cypher method
// Includes keys, infrastructutre information etc
// If many encryption methods will be used it worth
// to extract interface
type OpenPGPCrypter struct {
	Configured bool

	KeyRingId      string
	IsUseKeyRingId bool

	ArmoredKey      string
	IsUseArmoredKey bool

	ArmoredKeyPath      string
	IsUseArmoredKeyPath bool

	PubKey    openpgp.EntityList
	SecretKey openpgp.EntityList
}

func (crypter *OpenPGPCrypter) IsArmed() bool {
	if crypter.IsUseKeyRingId {
		tracelog.WarningLogger.Println(`
You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set GPG key using environment variables WALG_PGP_KEY or WALG_PGP_KEY_PATH.
		`)
	}

	return crypter.IsUseArmoredKey || crypter.IsUseArmoredKeyPath || crypter.IsUseKeyRingId
}

// IsUsed is to check necessity of Crypter use
// Must be called prior to any other crypter call
func (crypter *OpenPGPCrypter) IsUsed() bool {
	if !crypter.Configured {
		crypter.ConfigurePGPCrypter()
	}

	return crypter.IsArmed()
}

// OpenPGPCrypter internal initialization
func (crypter *OpenPGPCrypter) ConfigurePGPCrypter() {
	crypter.Configured = true

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

// Encrypt creates encryption writer from ordinary writer
func (crypter *OpenPGPCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}

	if crypter.PubKey == nil {
		if crypter.IsUseArmoredKey {
			entityList, err := openpgp.ReadArmoredKeyRing(strings.NewReader(crypter.ArmoredKey))

			if err != nil {
				return nil, err
			}

			crypter.PubKey = entityList
		} else if crypter.IsUseArmoredKeyPath {
			entityList, err := ReadPGPKey(crypter.ArmoredKeyPath)

			if err != nil {
				return nil, err
			}

			crypter.PubKey = entityList
		} else {
			// TODO: legacy gpg external use, need to remove in next major version
			armor, err := getPubRingArmor(crypter.KeyRingId)

			if err != nil {
				return nil, err
			}

			entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armor))

			if err != nil {
				return nil, err
			}

			crypter.PubKey = entityList
		}
	}

	return &DelayWriteCloser{writer, crypter, nil}, nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *OpenPGPCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}

	if crypter.SecretKey == nil {
		if crypter.IsUseArmoredKey {
			entityList, err := openpgp.ReadArmoredKeyRing(strings.NewReader(crypter.ArmoredKey))

			if err != nil {
				return nil, err
			}

			crypter.SecretKey = entityList
		} else if crypter.IsUseArmoredKeyPath {
			entityList, err := ReadPGPKey(crypter.ArmoredKeyPath)

			if err != nil {
				return nil, err
			}

			crypter.SecretKey = entityList
		} else {
			// TODO: legacy gpg external use, need to remove in next major version
			armor, err := getSecretRingArmor(crypter.KeyRingId)

			if err != nil {
				return nil, err
			}

			entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armor))

			if err != nil {
				return nil, err
			}

			crypter.SecretKey = entityList
		}

		if passphrase, isExist := LookupConfigValue("WALG_PGP_KEY_PASSPHRASE"); isExist {
			err := DecryptSecretKey(crypter.SecretKey, passphrase)

			if err != nil {
				return nil, err
			}
		}
	}

	md, err := openpgp.ReadMessage(reader, crypter.SecretKey, nil, nil)

	if err != nil {
		return nil, err
	}

	return md.UnverifiedBody, nil
}

func (crypter *OpenPGPCrypter) WrapWriter(writer io.WriteCloser) (io.WriteCloser, error) {
	return openpgp.Encrypt(writer, crypter.PubKey, nil, nil, nil)
}

func (crypter *OpenPGPCrypter) GetType() string {
	return "openpgp"
}
