package yckms

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/kms/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/go-sdk/iamkey"

	"github.com/wal-g/wal-g/internal/crypto/envelope"
)

const (
	magic              = "envelope-yc-kms"
	schemeVersion byte = 1
)

type Enveloper struct {
	keyID string
	sdk   *ycsdk.SDK
}

func (enveloper *Enveloper) GetName() string {
	return "yckms"
}

func (enveloper *Enveloper) GetEncryptedKey(r io.Reader) ([]byte, error) {
	encryptedKey, err := readEncryptedKey(r)
	if err != nil {
		return nil, err
	}
	return encryptedKey, nil
}

func (enveloper *Enveloper) DecryptKey(encryptedKey []byte) ([]byte, error) {
	ctx := context.Background()
	rsp, err := enveloper.sdk.KMSCrypto().SymmetricCrypto().Decrypt(ctx, &kms.SymmetricDecryptRequest{
		KeyId:      enveloper.keyID,
		Ciphertext: encryptedKey,
		AadContext: nil,
	})

	if err != nil {
		return nil, err
	}

	return rsp.Plaintext, nil
}

func (enveloper *Enveloper) SerializeEncryptedKey(encryptedKey []byte) []byte {
	return serializeEncryptedKey(encryptedKey)
}

func serializeEncryptedKey(encryptedKey []byte) []byte {
	/*
		magic value "envelope-yc-kms"
		scheme version (current version is 1)
		uint32 - encrypted key len
		encrypted key ...
	*/

	encryptedKeyLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(encryptedKeyLen, uint32(len(encryptedKey)))
	result := append([]byte(magic), schemeVersion)
	result = append(result, encryptedKeyLen...)

	return append(result, encryptedKey...)
}

func readEncryptedKey(r io.Reader) ([]byte, error) {
	magicSchemeBytes := make([]byte, len(magic)+1)
	_, err := r.Read(magicSchemeBytes)
	if err != nil {
		return nil, err
	}

	if string(magicSchemeBytes[0:len(magic)]) != magic {
		return nil, errors.New("envelope yc kms: invalid encrypted header format")
	}

	if schemeVersion != magicSchemeBytes[len(magic)] {
		return nil, errors.New("envelope yc kms: scheme version is not supported")
	}

	encryptedKeyLenBytes := make([]byte, 4)
	_, err = r.Read(encryptedKeyLenBytes)
	if err != nil {
		return nil, err
	}

	encryptedKeyLen := binary.LittleEndian.Uint32(encryptedKeyLenBytes)
	encryptedKey := make([]byte, encryptedKeyLen)
	_, err = r.Read(encryptedKey)
	if err != nil {
		return nil, err
	}

	return encryptedKey, nil
}

func EnveloperFromKeyIDAndCredential(keyID string, saFilePath string) (envelope.Enveloper, error) {
	var err error
	authorizedKey, err := iamkey.ReadFromJSONFile(saFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "Can't initialize yc sdk")
	}
	credentials, err := ycsdk.ServiceAccountKey(authorizedKey)
	if err != nil {
		return nil, errors.Wrap(err, "Can't initialize yc sdk")
	}

	sdk, err := ycsdk.Build(context.Background(), ycsdk.Config{
		Credentials: credentials,
	})
	if err != nil {
		return nil, errors.Wrap(err, "Can't initialize yc sdk")
	}
	return &Enveloper{
		keyID: keyID,
		sdk:   sdk,
	}, nil
}
