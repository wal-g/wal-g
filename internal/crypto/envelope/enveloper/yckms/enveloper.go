package yckms

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/kms/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/go-sdk/iamkey"

	"github.com/wal-g/tracelog"
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

func (enveloper *Enveloper) Name() string {
	return "yckms"
}

func (enveloper *Enveloper) ReadEncryptedKey(r io.Reader) ([]byte, error) {
	return readEncryptedKey(r)
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

func (enveloper *Enveloper) SerializeEncryptedKey(encryptedKey []byte, keyID string) []byte {
	return serializeEncryptedKey(encryptedKey, keyID)
}

func serializeEncryptedKey(encryptedKey []byte, keyID string) []byte {
	/*
		magic value "envelope-yc-kms"
		scheme version (current version is 1)
		uint32 - keyID len
		keyID ...
		uint32 - encrypted key len
		encrypted key ...
	*/

	result := append([]byte(magic), schemeVersion)

	keyIDLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyIDLen, uint32(len(keyID)))
	result = append(result, keyIDLen...)
	result = append(result, []byte(keyID)...)

	encryptedKeyLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(encryptedKeyLen, uint32(len(encryptedKey)))
	result = append(result, encryptedKeyLen...)
	return append(result, encryptedKey...)
}

func readEncryptedKey(r io.Reader) ([]byte, error) {
	magicSchemeBytes := make([]byte, len(magic)+1)
	_, err := io.ReadFull(r, magicSchemeBytes)
	if err != nil {
		return nil, err
	}

	if string(magicSchemeBytes[0:len(magic)]) != magic {
		return nil, errors.New("envelope yc kms: invalid encrypted header format")
	}

	if schemeVersion != magicSchemeBytes[len(magic)] {
		return nil, errors.New("envelope yc kms: scheme version is not supported")
	}

	keyIDLenBytes := make([]byte, 4)
	_, err = io.ReadFull(r, keyIDLenBytes)
	if err != nil {
		return nil, err
	}

	keyIDLen := binary.LittleEndian.Uint32(keyIDLenBytes)
	keyIDBytes := make([]byte, keyIDLen)

	_, err = io.ReadFull(r, keyIDBytes)
	if err != nil {
		return nil, err
	}
	keyID := string(keyIDBytes)
	tracelog.DebugLogger.Printf("Encrypted key was found: %s\n", keyID)

	encryptedKeyLenBytes := make([]byte, 4)
	_, err = io.ReadFull(r, encryptedKeyLenBytes)
	if err != nil {
		return nil, err
	}

	encryptedKeyLen := binary.LittleEndian.Uint32(encryptedKeyLenBytes)
	encryptedKey := make([]byte, encryptedKeyLen)

	_, err = io.ReadFull(r, encryptedKey)
	if err != nil {
		return nil, err
	}

	return encryptedKey, nil
}

func getCredentials(saFilePath string) (ycsdk.Credentials, error) {
	var credentials ycsdk.Credentials
	credentials = ycsdk.InstanceServiceAccount()
	if len(saFilePath) > 0 {
		var authorizedKey, keyErr = iamkey.ReadFromJSONFile(saFilePath)
		if keyErr != nil {
			return nil, errors.Wrap(keyErr, "Can't initialize yc sdk")
		}
		var accountCredentials, credErr = ycsdk.ServiceAccountKey(authorizedKey)
		if credErr != nil {
			return nil, errors.Wrap(credErr, "Can't initialize yc sdk")
		}
		credentials = accountCredentials
	}
	return credentials, nil
}

func EnveloperFromKeyIDAndCredential(keyID, saFilePath, endpoint string) (envelope.Enveloper, error) {
	credentials, credErr := getCredentials(saFilePath)
	if credErr != nil {
		return nil, errors.Wrap(credErr, "Can't initialize yc sdk")
	}

	var sdk, sdkErr = ycsdk.Build(context.Background(), ycsdk.Config{
		Credentials: credentials,
		Endpoint:    endpoint,
	})
	if sdkErr != nil {
		return nil, errors.Wrap(sdkErr, "Can't initialize yc sdk")
	}
	return &Enveloper{
		keyID: keyID,
		sdk:   sdk,
	}, nil
}
