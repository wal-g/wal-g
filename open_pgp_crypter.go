package walg

import (
	"golang.org/x/crypto/openpgp"
	"errors"
	"io"
	"bytes"
)

// OpenPGPCrypter incapsulates specific of cypher method
// Includes keys, infrastructutre information etc
// If many encryption methods will be used it worth
// to extract interface
type OpenPGPCrypter struct {
	configured, armed bool
	keyRingId         string

	pubKey    openpgp.EntityList
	secretKey openpgp.EntityList
}

// IsUsed is to check necessity of Crypter use
// Must be called prior to any other crypter call
func (crypter *OpenPGPCrypter) IsUsed() bool {
	if !crypter.configured {
		crypter.ConfigureGPGCrypter()
	}
	return crypter.armed
}

// ConfigureGPGCrypter is OpenPGPCrypter internal initialization
func (crypter *OpenPGPCrypter) ConfigureGPGCrypter() {
	crypter.configured = true
	crypter.keyRingId = GetKeyRingId()
	crypter.armed = len(crypter.keyRingId) != 0
}

// ErrCrypterUseMischief happens when crypter is used before initialization
var ErrCrypterUseMischief = errors.New("Crypter is not checked before use")

// Encrypt creates encryption writer from ordinary writer
func (crypter *OpenPGPCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	if !crypter.configured {
		return nil, ErrCrypterUseMischief
	}
	if crypter.pubKey == nil {
		armour, err := getPubRingArmour(crypter.keyRingId)
		if err != nil {
			return nil, err
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.pubKey = entitylist
	}

	return &DelayWriteCloser{writer, crypter.pubKey, nil}, nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *OpenPGPCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	if !crypter.configured {
		return nil, ErrCrypterUseMischief
	}
	if crypter.secretKey == nil {
		armour, err := getSecretRingArmour(crypter.keyRingId)
		if err != nil {
			return nil, err
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.secretKey = entitylist
	}

	var md, err0 = openpgp.ReadMessage(reader, crypter.secretKey, nil, nil)
	if err0 != nil {
		return nil, err0
	}

	return md.UnverifiedBody, nil
}
