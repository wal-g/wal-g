package yckms

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/yandex-cloud/go-genproto/yandex/cloud/kms/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
)

const (
	magic              = "yckms"
	schemeVersion byte = 1
)

type ycSymmetricKeyInterface interface {
	GetKey() []byte
	Decrypt() error
	GetEncryptedKey() []byte
	ReadEncryptedKey(r io.Reader) error
	CreateKey() error
}

type ycSymmetricKey struct {
	keyID        string
	key          []byte
	encryptedKey []byte

	sdk *ycsdk.SDK
}

func serializeEncryptedKey(encryptedKey []byte) []byte {
	/*
		magic value "yckms"
		scheme version (current version is 1)
		uint64 - encrypted key len
		encrypted key ...
	*/

	encryptedKeyLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(encryptedKeyLen, uint32(len(encryptedKey)))
	result := append([]byte(magic), schemeVersion)
	result = append(result, encryptedKeyLen...)

	return append(result, encryptedKey...)
}

func deserializeEncryptedKey(r io.Reader) ([]byte, error) {
	magicSchemeBytes := make([]byte, len(magic)+1)
	_, err := r.Read(magicSchemeBytes)
	if err != nil {
		return nil, err
	}

	if string(magicSchemeBytes[0:len(magic)]) != magic {
		return nil, errors.New("YC KMS: invalid encrypted header format")
	}

	if schemeVersion != magicSchemeBytes[len(magic)] {
		return nil, errors.New("YC KMS: scheme version is not supported")
	}

	encryptedKeyLenBytes := make([]byte, 4)
	_, err = r.Read(encryptedKeyLenBytes)
	if err != nil {
		return nil, err
	}

	encryptedKeyLen := binary.LittleEndian.Uint32(encryptedKeyLenBytes)
	/*
		Sanity check
	*/
	if encryptedKeyLen > 4096 {
		return nil, errors.New("YC KMS: invalid size of the encrypted key")
	}

	encryptedKey := make([]byte, encryptedKeyLen)
	_, err = r.Read(encryptedKey)
	if err != nil {
		return nil, err
	}

	return encryptedKey, nil
}

func (key *ycSymmetricKey) GetKey() []byte {
	return key.key
}

func (key *ycSymmetricKey) Decrypt() error {
	ctx := context.Background()
	rsp, err := key.sdk.KMSCrypto().SymmetricCrypto().Decrypt(ctx, &kms.SymmetricDecryptRequest{
		KeyId:      key.keyID,
		AadContext: nil,
		Ciphertext: key.encryptedKey,
	})

	if err != nil {
		return err
	}

	key.key = rsp.Plaintext
	return nil
}

func (key *ycSymmetricKey) GetEncryptedKey() []byte {
	if key.encryptedKey != nil {
		return serializeEncryptedKey(key.encryptedKey)
	}
	return nil
}

func (key *ycSymmetricKey) ReadEncryptedKey(r io.Reader) error {
	encryptedKey, err := deserializeEncryptedKey(r)
	if err == nil {
		key.encryptedKey = encryptedKey
	}
	return err
}

func (key *ycSymmetricKey) CreateKey() error {
	ctx := context.Background()
	dataKeyResponse, err := key.sdk.KMSCrypto().SymmetricCrypto().GenerateDataKey(ctx, &kms.GenerateDataKeyRequest{
		KeyId:         key.keyID,
		DataKeySpec:   kms.SymmetricAlgorithm_AES_256,
		SkipPlaintext: false,
	})
	if err != nil {
		return err
	}

	key.encryptedKey = dataKeyResponse.DataKeyCiphertext
	key.key = dataKeyResponse.DataKeyPlaintext
	return nil
}

func YcSymmetricKeyFromKeyIDAndSdk(keyID string, sdk *ycsdk.SDK) ycSymmetricKeyInterface {
	return &ycSymmetricKey{
		keyID:        keyID,
		key:          nil,
		encryptedKey: nil,
		sdk:          sdk,
	}
}
