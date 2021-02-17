package yckms

import (
	"bufio"
	"context"
	"io"

	"github.com/minio/sio"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/ioextensions"
	ycsdk "github.com/yandex-cloud/go-sdk"
)

type YcCrypter struct {
	symmetricKey ycSymmetricKeyInterface
}

func (crypter *YcCrypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	if crypter.symmetricKey.GetKey() == nil {
		err := crypter.symmetricKey.CreateKey()
		tracelog.ErrorLogger.FatalfOnError("Can't generate symmetric key: %v", err)
	}

	bufferedWriter := bufio.NewWriter(writer)
	_, err := bufferedWriter.Write(crypter.symmetricKey.GetEncryptedKey())

	if err != nil {
		tracelog.ErrorLogger.Printf("Can't write encryption key to buffer: %v", err)
		return nil, err
	}

	encryptedWriter, err := sio.EncryptWriter(bufferedWriter, sio.Config{Key: crypter.symmetricKey.GetKey(), CipherSuites: []byte{sio.AES_256_GCM}})

	if err != nil {
		tracelog.ErrorLogger.Printf("YC KMS can't create encrypted writer: %v", err)
		return nil, err
	}

	return ioextensions.NewOnCloseFlusher(encryptedWriter, bufferedWriter), nil
}

func (crypter *YcCrypter) Decrypt(reader io.Reader) (io.Reader, error) {
	err := crypter.symmetricKey.ReadEncryptedKey(reader)
	tracelog.ErrorLogger.FatalfOnError("Can't read encryption key from archive file header: %v", err)

	err = crypter.symmetricKey.Decrypt()
	tracelog.ErrorLogger.FatalfOnError("Can't decrypt data encryption key from archive file header: %v", err)

	return sio.DecryptReader(reader, sio.Config{Key: crypter.symmetricKey.GetKey(), CipherSuites: []byte{sio.AES_256_GCM}})
}

func YcCrypterFromKeyIdAndCredential(keyId string, saFilePath string) crypto.Crypter {
	credentials := resolveCredentials(saFilePath)
	sdk, err := ycsdk.Build(context.Background(), ycsdk.Config{
		Credentials: credentials,
	})
	tracelog.ErrorLogger.FatalfOnError("Can't initialize yc sdk: %v", err)

	return &YcCrypter{symmetricKey: YcSymmetricKeyFromKeyIdAndSdk(keyId, sdk)}
}
