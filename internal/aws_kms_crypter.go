package internal

import (
	"crypto/rand"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/minio/sio"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"sync"
)

type AWSKMSCrypter struct {
	Configured bool
	KMSKeyId   string

	SymmetricKey             []byte
	SymmetricKeyLen          int
	EncryptedSymmetricKey    []byte
	EncryptedSymmetricKeyLen int

	mutex sync.RWMutex
}

// ConfigureKMSCrypter is AWSKMSCrypter internal initialization
func (crypter *AWSKMSCrypter) ConfigureKMSCrypter() {
	crypter.Configured = true
	crypter.SymmetricKeyLen = 32
	crypter.EncryptedSymmetricKeyLen = 184
	crypter.KMSKeyId = GetKMSKeyId()
}

func (crypter *AWSKMSCrypter) IsArmed() bool {
	return len(crypter.KMSKeyId) != 0
}

// IsUsed is to check necessity of Crypter use
// Must be called prior to any other crypter call
func (crypter *AWSKMSCrypter) IsUsed() bool {
	if !crypter.Configured {
		crypter.ConfigureKMSCrypter()
	}
	return crypter.IsArmed()
}

// Generate symmetric key
func (crypter *AWSKMSCrypter) GenerateSymmetricKey(keyLen int) error {
	key := make([]byte, keyLen)
	_, err := rand.Read(key)
	if err == nil {
		crypter.mutex.Lock()
		crypter.SymmetricKey = key
		crypter.mutex.Unlock()
	}
	return err
}

// Encrypt symmetric key with AWS KMS
func (crypter *AWSKMSCrypter) EncryptSymmetricKey() error {
	svc := kms.New(session.New())

	crypter.mutex.RLock()
	input := &kms.EncryptInput{
		KeyId:     aws.String(crypter.KMSKeyId),
		Plaintext: crypter.SymmetricKey,
	}
	crypter.mutex.RUnlock()

	result, err := svc.Encrypt(input)

	if err == nil {
		crypter.mutex.Lock()
		crypter.EncryptedSymmetricKey = result.CiphertextBlob
		crypter.mutex.Unlock()
	}

	return err
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *AWSKMSCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}
	if len(crypter.SymmetricKey) == 0 {
		err := crypter.GenerateSymmetricKey(crypter.SymmetricKeyLen)
		if err != nil {
			tracelog.ErrorLogger.Printf("Can't generate symmetric key: %v", err)
		}

		err = crypter.EncryptSymmetricKey()
		if err != nil {
			tracelog.ErrorLogger.Printf("Can't encrypt symmetric key: %v", err)
		}
	}

	return &DelayWriteCloser{writer, crypter, nil}, nil
}

// Decrypt symmetric encryption key with AWS KMS
func (crypter *AWSKMSCrypter) DecryptSymmetricKey() error {
	svc := kms.New(session.New())

	crypter.mutex.RLock()
	input := &kms.DecryptInput{
		CiphertextBlob: crypter.EncryptedSymmetricKey,
	}
	crypter.mutex.RUnlock()

	result, err := svc.Decrypt(input)

	if err == nil {
		crypter.mutex.Lock()
		crypter.SymmetricKey = result.Plaintext
		crypter.mutex.Unlock()
	}

	return err
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *AWSKMSCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {

	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}

	encryptedSymmetricKey := make([]byte, crypter.EncryptedSymmetricKeyLen)
	_, err := reader.Read(encryptedSymmetricKey)
	if err != nil {
		tracelog.ErrorLogger.Printf("Can't read encryption key from s3: %v", err)
		return reader, err
	}

	crypter.mutex.Lock()
	crypter.EncryptedSymmetricKey = encryptedSymmetricKey
	crypter.mutex.Unlock()

	err = crypter.DecryptSymmetricKey()
	if err != nil {
		tracelog.ErrorLogger.Printf("Can't decrypt symmetric key: %v", err)
	}

	crypter.mutex.RLock()
	defer crypter.mutex.RUnlock()

	return sio.DecryptReader(reader, sio.Config{Key: crypter.SymmetricKey})
}

// Wrap writer with symmetric encryption
func (crypter *AWSKMSCrypter) WrapWriter(writer io.WriteCloser) (io.WriteCloser, error) {
	crypter.mutex.RLock()
	_, err := writer.Write(crypter.EncryptedSymmetricKey)
	crypter.mutex.RUnlock()
	if err != nil {
		tracelog.ErrorLogger.Printf("Can't write encryption key to s3: %v", err)
		return writer, err
	}

	crypter.mutex.RLock()
	defer crypter.mutex.RUnlock()

	return sio.EncryptWriter(writer, sio.Config{Key: crypter.SymmetricKey})
}

func (crypter *AWSKMSCrypter) GetType() string {
	return "aws-kms"
}
