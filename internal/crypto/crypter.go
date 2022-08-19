package crypto

import "io"

// Crypter is responsible for making cryptographical pipeline parts when needed
type Crypter interface {
	Name() string
	Encrypt(writer io.Writer) (io.WriteCloser, error)
	Decrypt(reader io.Reader) (io.Reader, error)
}
