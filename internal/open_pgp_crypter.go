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
		armour, err := getPubRingArmour(crypter.KeyRingId)
		if err != nil {
			return nil, err
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.PubKey = entitylist
	}

	return &DelayWriteCloser{writer, crypter, nil}, nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *OpenPGPCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}
	if crypter.SecretKey == nil {
		armour, err := getSecretRingArmour(crypter.KeyRingId)
		if err != nil {
			return nil, err
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.SecretKey = entitylist
	}

	var md, err0 = openpgp.ReadMessage(reader, crypter.SecretKey, nil, nil)
	if err0 != nil {
		return nil, err0
	}

	return md.UnverifiedBody, nil
}

func (crypter *OpenPGPCrypter) ForceEncrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	return openpgp.Encrypt(writer, crypter.PubKey, nil, nil, nil)
}
