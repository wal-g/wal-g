package internal

import "io"

// Crypter is responsible for making cryptographical pipeline parts when needed
type Crypter interface {
	IsUsed() bool
	Encrypt(writer io.WriteCloser) (io.WriteCloser, error)
	Decrypt(reader io.ReadCloser) (io.Reader, error)
	WrapWriter(writer io.WriteCloser) (io.WriteCloser, error)
	GetType() string
}
