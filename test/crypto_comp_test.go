// +build lzo

package test

import (
	"bytes"
	"github.com/wal-g/wal-g/internal/crypto"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/compression/lzo"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
)

const (
	waleWALfilename    = "testdata/000000010000000000000024.lzo"
	waleGpgKeyFilePath = "./testdata/waleGpgKey"
	gpgKeyID           = "walg-server-test"
)

var waleGpgKey string

func init() {
	waleGpgKeyBytes, err := ioutil.ReadFile(waleGpgKeyFilePath)
	if err != nil {
		panic(err)
	}
	waleGpgKey = string(waleGpgKeyBytes)
}

func noPassphrase() (string, bool) {
	return "", false
}

// This test extracts WAL-E-encrypted WAL, decrypts it by external
// GPG, compares result with OpenGPG decryption and invokes Lzop
// decompression to check integrity. Test will leave gpg key
// "walg-server-test" installed.
func TestDecryptWALElzo(t *testing.T) {
	t.Skip("This test has gpg side effects and was skipped. If you want to run it - comment skip line in crypto_compt_test.go")

	crypter := openpgp.CrypterFromKey(waleGpgKey, noPassphrase)
	f, err := os.Open(waleWALfilename)
	assert.NoError(t, err)
	decrypt, err := crypter.Decrypt(f)
	assert.NoError(t, err)
	bytes1, err := ioutil.ReadAll(decrypt)
	assert.NoError(t, err)

	installTestKeyToExternalGPG(t)

	ec := &ExternalGPGCrypter{}

	f, err = os.Open(waleWALfilename)
	assert.NoError(t, err)
	bytes2, err := ec.Decrypt(f)
	assert.NoError(t, err)

	assert.Equalf(t, bytes1, bytes2, "Decryption result differ")

	buffer := bytes.Buffer{}
	decompressor := lzo.Decompressor{}
	err = decompressor.Decompress(&buffer, bytes.NewReader(bytes1))
	assert.NoError(t, err)

	/* Unfortunately, we cannot quietly uninstall test keyring. This is why this test is not executed by default.
	command = exec.Command(gpgBin, "--delete-secret-key", "--yes", "D32100BF1CDA62E5E50008F751EFFF0B6548E47F")
	_, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}*/
}
func installTestKeyToExternalGPG(t *testing.T) *exec.Cmd {
	command := exec.Command(crypto.GpgBin, "--import")

	command.Stdin = strings.NewReader(waleGpgKey)
	err := command.Run()
	assert.NoError(t, err)
	return command
}

// This test encrypts test data by GPG installed into current
// system (which would be used by WAL-E) and decrypts by OpenGPG used by WAL-G
// To run this test you have to trust key "walg-server-test":
// gpg --edit-key wal-server-test
// trust
// 5
// quit

// Test will leave gpg key "walg-server-test" installed.
func TestOpenGPGandExternalGPGCompatibility(t *testing.T) {
	t.Skip("This test has gpg side effects and was skipped. If you want to run it - comment skip line in crypto_compt_test.go")

	installTestKeyToExternalGPG(t)

	ec := &ExternalGPGCrypter{}
	c := openpgp.CrypterFromKeyRingID(gpgKeyID, noPassphrase)

	assert.NotNilf(t, c, "OpenGPG crypter is unable to initialize")

	for i := uint(0); i < 16; i++ {
		tokenSize := 512 << i
		token := make([]byte, tokenSize)
		rand.Read(token)

		bytes1, err := ec.Encrypt(bytes.NewReader(token))
		assert.NoError(t, err)

		reader, err := c.Decrypt(bytes.NewReader(bytes1))

		assert.NoError(t, err)

		decrypted, err := ioutil.ReadAll(reader)

		assert.NoError(t, err)

		assert.Equal(t, token, decrypted, "OpenGPG could not decrypt GPG produced result for chumk of size ", tokenSize)
	}
}

type ExternalGPGCrypter struct {
}

func (c *ExternalGPGCrypter) Encrypt(reader io.Reader) ([]byte, error) {
	cmd := exec.Command("gpg", "-e", "-z", "0", "-r", gpgKeyID)

	cmd.Stdin = reader

	return cmd.Output()
}

func (c *ExternalGPGCrypter) Decrypt(reader io.Reader) ([]byte, error) {
	cmd := exec.Command("gpg", "-d", "-q", "--batch")

	cmd.Stdin = reader

	return cmd.Output()
}
