package walg

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"golang.org/x/crypto/openpgp"
)

const pgpTestPrivateKey string = `
-----BEGIN PGP PRIVATE KEY BLOCK-----

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
nWM=
=UysV
-----END PGP PRIVATE KEY BLOCK-----
`

func MockArmedCrypter() Crypter {
	return createCrypter(pgpTestPrivateKey)
}
func createCrypter(armedKeyring string) *OpenPGPCrypter {
	ring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armedKeyring))
	if err != nil {
		panic(err)
	}
	crypter := &OpenPGPCrypter{armed: true, configured: true, pubKey: ring, secretKey: ring}
	return crypter
}

func MockDisarmedCrypter() Crypter {
	return &MockCrypter{}
}

type MockCrypter struct {
}

func (crypter *MockCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	return writer, nil
}

func (crypter *MockCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	return reader, nil
}

func (crypter *MockCrypter) IsUsed() bool {
	return true
}

func TestMockCrypter(t *testing.T) {
	MockArmedCrypter()
	MockDisarmedCrypter()
}

type ClosingBuffer struct {
	*bytes.Buffer
}

func (cb *ClosingBuffer) Close() (err error) {
	return nil
}

func TestEncryptionCycle(t *testing.T) {
	crypter := MockArmedCrypter()
	const somesecret = "so very secret thingy"

	buf := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(&ClosingBuffer{buf})
	if err != nil {
		t.Errorf("Encryption error: %v", err)
	}

	encrypt.Write([]byte(somesecret))
	encrypt.Close()

	decrypt, err := crypter.Decrypt(&ClosingBuffer{buf})
	if err != nil {
		t.Errorf("Decryption error: %v", err)
	}

	decryptedBytes, err := ioutil.ReadAll(decrypt)
	if err != nil {
		t.Errorf("Decryption read error: %v", err)
	}

	if string(decryptedBytes) != somesecret {
		t.Errorf("Decrypted text not equals open text")
	}
}
