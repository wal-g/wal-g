package yckms

import (
	"bufio"
	"context"
	"github.com/minio/sio"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/ioextensions"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"io"
)

type YcCrypter struct {
	SymmetricKey ycSymmetricKeyInterface
}

func (crypter *YcCrypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	if crypter.SymmetricKey.GetKey() == nil {
		err := crypter.SymmetricKey.CreateKey()
		tracelog.ErrorLogger.FatalfOnError("Can't generate symmetric key: %v", err)
	}

	bufferedWriter := bufio.NewWriter(writer)
	_, err := bufferedWriter.Write(crypter.SymmetricKey.GetEncryptedKey())

	if err != nil {
		tracelog.ErrorLogger.Printf("Can't write encryption key to buffer: %v", err)
		return nil, err
	}

	encryptedWriter, err := sio.EncryptWriter(bufferedWriter, sio.Config{Key: crypter.SymmetricKey.GetKey(), CipherSuites: []byte{sio.AES_256_GCM}})

	if err != nil {
		tracelog.ErrorLogger.Printf("YC KMS can't create encrypted writer: %v", err)
		return nil, err
	}

	return ioextensions.NewOnCloseFlusher(encryptedWriter, bufferedWriter), nil
}

func (crypter *YcCrypter) Decrypt(reader io.Reader) (io.Reader, error) {
	err := crypter.SymmetricKey.ReadEncryptedKey(reader)
	tracelog.ErrorLogger.FatalfOnError("Can't read encryption key from archive file header: %v", err)

	err = crypter.SymmetricKey.Decrypt()
	tracelog.ErrorLogger.FatalfOnError("Can't decrypt data encryption key from archive file header: %v", err)

	return sio.DecryptReader(reader, sio.Config{Key: crypter.SymmetricKey.GetKey(), CipherSuites: []byte{sio.AES_256_GCM}})
}

func YcCrypterFromKeyIdAndCredential(keyId string, saFilePath string) crypto.Crypter {
	credentials := resolveCredentials(saFilePath)
	sdk, err := ycsdk.Build(context.Background(), ycsdk.Config{
		Credentials: credentials,
	})
	tracelog.ErrorLogger.FatalfOnError("Can't initialize yc sdk: %v", err)

	return &YcCrypter{SymmetricKey: YcSymmetricKeyFromKeyIdAndSdk(keyId, sdk)}
}
