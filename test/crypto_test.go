package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"golang.org/x/crypto/openpgp"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

var pgpTestPrivateKey string

const PrivateKeyFilePath = "./testdata/pgpTestPrivateKey"

func init() {
	pgpTestPrivateKeyBytes, err := ioutil.ReadFile(PrivateKeyFilePath)
	if err != nil {
		panic(err)
	}
	pgpTestPrivateKey = string(pgpTestPrivateKeyBytes)
}

func MockArmedCrypter() internal.Crypter {
	return createCrypter(pgpTestPrivateKey)
}

func createCrypter(armedKeyring string) *internal.OpenPGPCrypter {
	ring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armedKeyring))
	if err != nil {
		panic(err)
	}
	crypter := &internal.OpenPGPCrypter{Configured: true, PubKey: ring, SecretKey: ring}
	return crypter
}

func MockDisarmedCrypter() internal.Crypter {
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

func (crypter *MockCrypter) WrapWriter(writer io.WriteCloser) (io.WriteCloser, error) {
	return writer, nil
}

func (crypter *MockCrypter) GetType() string {
	return "mock"
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
	const someSecret = "so very secret thingy"

	buf := new(bytes.Buffer)
	encrypt, err := crypter.Encrypt(&ClosingBuffer{buf})
	assert.NoErrorf(t, err, "Encryption error: %v", err)

	encrypt.Write([]byte(someSecret))
	encrypt.Close()

	decrypt, err := crypter.Decrypt(&ClosingBuffer{buf})
	assert.NoErrorf(t, err, "Decryption error: %v", err)

	decryptedBytes, err := ioutil.ReadAll(decrypt)
	assert.NoErrorf(t, err, "Decryption read error: %v", err)

	assert.Equal(t, someSecret, string(decryptedBytes), "Decrypted text not equals open text")
}

func TestNewCrypter(t *testing.T) {
	// test OpenPGPCrypter choice

	// check by key id
	// clean envs
	err := os.Unsetenv("WALG_GPG_KEY_ID")
	if err != nil {
		t.Log(err)
	}
	// prepare openpgp crypter WALG_GPG_KEY_ID env var
	err = os.Setenv("WALG_GPG_KEY_ID", "WALG_GPG_KEY_ID")
	if err != nil {
		t.Log(err)
	}
	// create pgp crypter
	crypter := internal.NewCrypter()
	// check pgp crypter type
	assert.Equal(t, "openpgp", crypter.GetType(), "Choosing pgp encryption with WALG_GPG_KEY_ID not working")
	// clean envs
	err = os.Unsetenv("WALG_GPG_KEY_ID")
	if err != nil {
		t.Log(err)
	}

	// check by key
	// clean envs
	err = os.Unsetenv("WALG_PGP_KEY")
	if err != nil {
		t.Log(err)
	}
	// prepare openpgp crypter with WALG_PGP_KEY env var
	err = os.Setenv("WALG_PGP_KEY", "WALG_PGP_KEY")
	if err != nil {
		t.Log(err)
	}
	// create pgp crypter
	crypter = internal.NewCrypter()
	// check pgp crypter type
	assert.Equal(t, "openpgp", crypter.GetType(), "Choosing pgp encryption with WALG_PGP_KEY not working")
	// clean envs
	err = os.Unsetenv("WALG_PGP_KEY")
	if err != nil {
		t.Log(err)
	}

	// check by key path
	// clean envs
	err = os.Unsetenv("WALG_PGP_KEY_PATH")
	if err != nil {
		t.Log(err)
	}
	// prepare openpgp crypter with WALG_PGP_KEY_PATH env var
	err = os.Setenv("WALG_PGP_KEY_PATH", "WALG_PGP_KEY_PATH")
	if err != nil {
		t.Log(err)
	}
	// create pgp crypter
	crypter = internal.NewCrypter()
	// check pgp crypter type
	assert.Equal(t, "openpgp", crypter.GetType(), "Choosing pgp encryption with WALG_PGP_KEY_PATH not working")
	// clean envs
	err = os.Unsetenv("WALG_PGP_KEY_PATH")
	if err != nil {
		t.Log(err)
	}

	// test AWSKMSCrypter choice
	// clean envs
	err = os.Unsetenv("WALG_CSE_KMS_ID")
	if err != nil {
		t.Log(err)
	}
	// prepare aws kms crypter with WALG_CSE_KMS_ID env var
	err = os.Setenv("WALG_CSE_KMS_ID", "WALG_CSE_KMS_ID")
	if err != nil {
		t.Log(err)
	}
	// create aws kms crypter
	crypter = internal.NewCrypter()
	// check aws kms crypter type
	assert.Equal(t, "aws-kms", crypter.GetType(), "Choosing aws kms encryption with WALG_CSE_KMS_ID not working")
	// clean envs
	err = os.Unsetenv("WALG_CSE_KMS_ID")
	if err != nil {
		t.Log(err)
	}

	// test default crypter choice
	// create crypter
	crypter = internal.NewCrypter()
	// check default crypter type
	assert.Equal(t, "openpgp", crypter.GetType(), "Choosing default encryption not working")
}
