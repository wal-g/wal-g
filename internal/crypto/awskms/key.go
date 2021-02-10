package awskms

import (
	"crypto/rand"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
)

// SymmetricKey is AWS KMS implementation of crypto.SymmetricKey interface
type SymmetricKey struct {
	SymmetricKey             []byte
	SymmetricKeyLen          int
	EncryptedSymmetricKey    []byte
	EncryptedSymmetricKeyLen int

	KeyID  string
	Region string

	mutex sync.RWMutex
}

// Generate symmetric key
func (symmetricKey *SymmetricKey) Generate() error {
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
func (symmetricKey *SymmetricKey) Encrypt() error {
	kmsConfig := aws.NewConfig()

	if symmetricKey.Region != "" {
		kmsConfig = kmsConfig.WithRegion(symmetricKey.Region)
	}

	kmsSession, err := session.NewSession()
	if err != nil {
		return err
	}

	svc := kms.New(kmsSession, kmsConfig)

	symmetricKey.mutex.RLock()
	input := &kms.EncryptInput{
		KeyId:     aws.String(symmetricKey.KeyID),
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

// Decrypt symmetric key with AWS KMS
func (symmetricKey *SymmetricKey) Decrypt() error {
	kmsConfig := aws.NewConfig()

	if symmetricKey.Region != "" {
		kmsConfig = kmsConfig.WithRegion(symmetricKey.Region)
	}

	kmsSession, err := session.NewSession()
	if err != nil {
		return err
	}

	svc := kms.New(kmsSession, kmsConfig)

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

// GetKey returna unencrypted symmetric key
func (symmetricKey *SymmetricKey) GetKey() []byte {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.SymmetricKey
}

// GetEncryptedKey returns encrypted symmetric key
func (symmetricKey *SymmetricKey) GetEncryptedKey() []byte {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.EncryptedSymmetricKey
}

// SetKey set unencrypted symmetric key
func (symmetricKey *SymmetricKey) SetKey(key []byte) error {
	symmetricKey.mutex.Lock()
	symmetricKey.SymmetricKey = key
	symmetricKey.mutex.Unlock()
	return nil
}

// SetEncryptedKey set encrypted symmetric key
func (symmetricKey *SymmetricKey) SetEncryptedKey(encryptedKey []byte) error {
	symmetricKey.mutex.Lock()
	symmetricKey.EncryptedSymmetricKey = encryptedKey
	symmetricKey.mutex.Unlock()
	return nil
}

// GetKeyID returns AWS KMS key ID
func (symmetricKey *SymmetricKey) GetKeyID() string {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.KeyID
}

// GetEncryptedKeyLen returns encrypted key length
func (symmetricKey *SymmetricKey) GetEncryptedKeyLen() int {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.EncryptedSymmetricKeyLen
}

// GetKeyLen returns key length
func (symmetricKey *SymmetricKey) GetKeyLen() int {
	symmetricKey.mutex.RLock()
	defer symmetricKey.mutex.RUnlock()
	return symmetricKey.SymmetricKeyLen
}

// NewSymmetricKey creates new symmetric AWS KMS key object
func NewSymmetricKey(kmsKeyID string, keyLen int, encryptedKeyLen int, kmsRegion string) *SymmetricKey {
	return &SymmetricKey{SymmetricKeyLen: keyLen, EncryptedSymmetricKeyLen: encryptedKeyLen, KeyID: kmsKeyID, Region: kmsRegion}
}
