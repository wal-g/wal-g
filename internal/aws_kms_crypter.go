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

type SymmetricKey interface {
	Generate() error
	Encrypt() error
	Decrypt() error
	GetKey() []byte
	SetKey([]byte) error
	GetEncryptedKey() []byte
	SetEncryptedKey([]byte) error
	GetKeyId() string
	GetEncryptedKeyLen() int
}

type AWSKMSSymmetricKey struct {
	SymmetricKey             []byte
	SymmetricKeyLen          int
	EncryptedSymmetricKey    []byte
	EncryptedSymmetricKeyLen int

	KeyId string

	mutex sync.RWMutex
}

// Generate symmetric key
func (symmetricKey *AWSKMSSymmetricKey) Generate() error {
	symmetricKey.mutex.RLock()
	key := make([]byte, symmetricKey.SymmetricKeyLen)
	symmetricKey.mutex.RUnlock()

	_, err := rand.Read(key)
	if err == nil {
		symmetricKey.mutex.Lock()
		symmetricKey.SymmetricKey = key
		symmetricKey.mutex.Unlock()
	}
	return err
}

// Encrypt symmetric key with AWS KMS
func (symmetricKey *AWSKMSSymmetricKey) Encrypt() error {
	svc := kms.New(session.New())

	symmetricKey.mutex.RLock()
	input := &kms.EncryptInput{
		KeyId:     aws.String(symmetricKey.KeyId),
		Plaintext: symmetricKey.SymmetricKey,
	}
	symmetricKey.mutex.RUnlock()

	result, err := svc.Encrypt(input)

	if err == nil {
		symmetricKey.mutex.Lock()
		symmetricKey.EncryptedSymmetricKey = result.CiphertextBlob
		symmetricKey.mutex.Unlock()
	}

	return err
}

// Decrypt symmetric encryption key with AWS KMS
func (symmetricKey *AWSKMSSymmetricKey) Decrypt() error {
	svc := kms.New(session.New())

	symmetricKey.mutex.RLock()
	input := &kms.DecryptInput{
		CiphertextBlob: symmetricKey.EncryptedSymmetricKey,
	}
	symmetricKey.mutex.RUnlock()

	result, err := svc.Decrypt(input)

	if err == nil {
		symmetricKey.mutex.Lock()
		symmetricKey.SymmetricKey = result.Plaintext
		symmetricKey.mutex.Unlock()
	}

	return err
}

func (symmetricKey *AWSKMSSymmetricKey) GetKey() []byte {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.SymmetricKey
}

func (symmetricKey *AWSKMSSymmetricKey) GetEncryptedKey() []byte {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.EncryptedSymmetricKey
}

func (symmetricKey *AWSKMSSymmetricKey) SetKey(key []byte) error {
	symmetricKey.mutex.Lock()
	symmetricKey.SymmetricKey = key
	symmetricKey.mutex.Unlock()
	return nil
}

func (symmetricKey *AWSKMSSymmetricKey) SetEncryptedKey(encryptedKey []byte) error {
	symmetricKey.mutex.Lock()
	symmetricKey.EncryptedSymmetricKey = encryptedKey
	symmetricKey.mutex.Unlock()
	return nil
}

func (symmetricKey *AWSKMSSymmetricKey) GetKeyId() string {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.KeyId
}

func (symmetricKey *AWSKMSSymmetricKey) GetEncryptedKeyLen() int {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.EncryptedSymmetricKeyLen
}

func NewSymmetricKey(kmsKeyId string, keyLen int, encryptedKeyLen int) SymmetricKey {
	return &AWSKMSSymmetricKey{SymmetricKeyLen: keyLen, EncryptedSymmetricKeyLen: encryptedKeyLen, KeyId: kmsKeyId}
}

type AWSKMSCrypter struct {
	Configured bool

	SymmetricKey SymmetricKey
}

// ConfigureKMSCrypter is AWSKMSCrypter internal initialization
func (crypter *AWSKMSCrypter) ConfigureKMSCrypter() {
	crypter.Configured = true

	crypter.SymmetricKey = NewSymmetricKey(GetKMSKeyId(), 32, 184)
}

func (crypter *AWSKMSCrypter) IsArmed() bool {
	return len(crypter.SymmetricKey.GetKeyId()) != 0
}

// IsUsed is to check necessity of Crypter use
// Must be called prior to any other crypter call
func (crypter *AWSKMSCrypter) IsUsed() bool {
	if !crypter.Configured {
		crypter.ConfigureKMSCrypter()
	}
	return crypter.IsArmed()
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *AWSKMSCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}
	if len(crypter.SymmetricKey.GetKey()) == 0 {
		err := crypter.SymmetricKey.Generate()
		if err != nil {
			tracelog.ErrorLogger.Printf("Can't generate symmetric key: %v", err)
		}

		err = crypter.SymmetricKey.Encrypt()
		if err != nil {
			tracelog.ErrorLogger.Printf("Can't encrypt symmetric key: %v", err)
		}
	}

	return &DelayWriteCloser{writer, crypter, nil}, nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *AWSKMSCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {

	if !crypter.Configured {
		return nil, NewCrypterUseMischiefError()
	}

	encryptedSymmetricKey := make([]byte, crypter.SymmetricKey.GetEncryptedKeyLen())
	_, err := reader.Read(encryptedSymmetricKey)
	if err != nil {
		tracelog.ErrorLogger.Printf("Can't read encryption key from s3: %v", err)
		return reader, err
	}

	crypter.SymmetricKey.SetEncryptedKey(encryptedSymmetricKey)

	err = crypter.SymmetricKey.Decrypt()
	if err != nil {
		tracelog.ErrorLogger.Printf("Can't decrypt symmetric key: %v", err)
	}

	return sio.DecryptReader(reader, sio.Config{Key: crypter.SymmetricKey.GetKey()})
}

// Wrap writer with symmetric encryption
func (crypter *AWSKMSCrypter) WrapWriter(writer io.WriteCloser) (io.WriteCloser, error) {
	_, err := writer.Write(crypter.SymmetricKey.GetEncryptedKey())
	if err != nil {
		tracelog.ErrorLogger.Printf("Can't write encryption key to s3: %v", err)
		return writer, err
	}

	return sio.EncryptWriter(writer, sio.Config{Key: crypter.SymmetricKey.GetKey()})
}

func (crypter *AWSKMSCrypter) GetType() string {
	return "aws-kms"
}
