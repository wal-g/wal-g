package internal

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/wal-g/wal-g/internal/tracelog"
	"golang.org/x/crypto/openpgp"
	"io"
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
	KeyRingId  string

	PubKey    openpgp.EntityList
	SecretKey openpgp.EntityList
}

func (crypter *OpenPGPCrypter) IsArmed() bool {
	return len(crypter.KeyRingId) != 0
}

// IsUsed is to check necessity of Crypter use
// Must be called prior to any other crypter call
func (crypter *OpenPGPCrypter) IsUsed() bool {
	if !crypter.Configured {
		crypter.ConfigureGPGCrypter()
	}
	return crypter.IsArmed()
}

// ConfigureGPGCrypter is OpenPGPCrypter internal initialization
func (crypter *OpenPGPCrypter) ConfigureGPGCrypter() {
	crypter.Configured = true
	crypter.KeyRingId = GetKeyRingId()
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *OpenPGPCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}

	if crypter.PubKey == nil {
		if pubKeyPath, isExist := LookupConfigValue("WALG_PGP_PUBLIC_KEY_PATH"); isExist {
			entityList, err := GetPGPKey(pubKeyPath)

			if err != nil {
				return nil, err
			}

			crypter.PubKey = entityList
		} else {
			// TODO: legacy gpg external use, need to remove in next major version
			tracelog.WarningLogger.Println(`
You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set gpg keys using environment variables WALG_PGP_PUBLIC_KEY_PATH.
			`)

			armour, err := getPubRingArmour(crypter.KeyRingId)
			if err != nil {
				return nil, err
			}

			entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
			if err != nil {
				return nil, err
			}

			crypter.PubKey = entityList
		}
	}

	return &DelayWriteCloser{writer, crypter.PubKey, nil}, nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *OpenPGPCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}

	if crypter.SecretKey == nil {
		if secKeyPath, isExist := LookupConfigValue("WALG_PGP_SECRET_KEY_PATH"); isExist {
			entityList, err := GetPGPKey(secKeyPath)

			if err != nil {
				return nil, err
			}

			crypter.SecretKey = entityList
		} else {
			// TODO: legacy gpg external use, need to remove in next major version
			tracelog.WarningLogger.Println(`
You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set gpg keys using environment variables WALG_PGP_SECRET_KEY_PATH.
			`)

			armour, err := getSecretRingArmour(crypter.KeyRingId)
			if err != nil {
				return nil, err
			}

			entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
			if err != nil {
				return nil, err
			}

			crypter.SecretKey = entityList
		}
	}

	md, err := openpgp.ReadMessage(reader, crypter.SecretKey, nil, nil)
	if err != nil {
		return nil, err
	}

	return md.UnverifiedBody, nil
}
