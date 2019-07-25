package crypto

// SymmetricKey encryption interface
// Used by AWS KMS Crypter
// When implement another crypters,
// can be moved to crypto package
type SymmetricKey interface {
	Generate() error
	Encrypt() error
	Decrypt() error
	GetKey() []byte
	SetKey([]byte) error
	GetEncryptedKey() []byte
	SetEncryptedKey([]byte) error
	GetKeyID() string
	GetEncryptedKeyLen() int
	GetKeyLen() int
}
