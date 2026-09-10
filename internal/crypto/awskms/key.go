package awskms

import (
	"context"
	"crypto/rand"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// SymmetricKey is AWS KMS implementation of crypto.SymmetricKey interface
type SymmetricKey struct {
	SymmetricKey             []byte
	SymmetricKeyLen          int
	EncryptedSymmetricKey    []byte
	EncryptedSymmetricKeyLen int

	KeyID  string
	Region string

	kms   *kms.Client
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

func (symmetricKey *SymmetricKey) client(ctx context.Context) (*kms.Client, error) {
	symmetricKey.mutex.RLock()
	svc := symmetricKey.kms
	symmetricKey.mutex.RUnlock()
	if svc != nil {
		return svc, nil
	}

	opts := []func(*config.LoadOptions) error{}
	if symmetricKey.Region != "" {
		opts = append(opts, config.WithRegion(symmetricKey.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	svc = kms.NewFromConfig(cfg)
	symmetricKey.mutex.Lock()
	symmetricKey.kms = svc
	symmetricKey.mutex.Unlock()
	return svc, nil
}

// Encrypt symmetric key with AWS KMS
func (symmetricKey *SymmetricKey) Encrypt(ctx context.Context) error {
	svc, err := symmetricKey.client(ctx)
	if err != nil {
		return err
	}

	symmetricKey.mutex.RLock()
	input := &kms.EncryptInput{
		KeyId:     aws.String(symmetricKey.KeyID),
		Plaintext: symmetricKey.SymmetricKey,
	}
	symmetricKey.mutex.RUnlock()

	result, err := svc.Encrypt(ctx, input)

	if err == nil {
		symmetricKey.mutex.Lock()
		symmetricKey.EncryptedSymmetricKey = result.CiphertextBlob
		symmetricKey.mutex.Unlock()
	}

	return err
}

// Decrypt symmetric key with AWS KMS
func (symmetricKey *SymmetricKey) Decrypt(ctx context.Context) error {
	svc, err := symmetricKey.client(ctx)
	if err != nil {
		return err
	}

	symmetricKey.mutex.RLock()
	input := &kms.DecryptInput{
		CiphertextBlob: symmetricKey.EncryptedSymmetricKey,
	}
	symmetricKey.mutex.RUnlock()

	result, err := svc.Decrypt(ctx, input)

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
	return &SymmetricKey{SymmetricKeyLen: keyLen,
		EncryptedSymmetricKeyLen: encryptedKeyLen,
		KeyID:                    kmsKeyID,
		Region:                   kmsRegion}
}
