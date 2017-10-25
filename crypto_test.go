package walg

import (
	"golang.org/x/crypto/openpgp"
	"strings"
	"testing"
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"io"
)

const pgpTestPrivateKey string = `
-----BEGIN PGP PRIVATE KEY BLOCK-----

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
nWM=
=UysV
-----END PGP PRIVATE KEY BLOCK-----
`

func MockArmedCrypter() (Crypter) {
	return mockCrypter(pgpTestPrivateKey)
}
func mockCrypter(armedKeyring string) *OpenPGPCrypter {
	ring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armedKeyring))
	if err != nil {
		panic(err)
	}
	crypter := &OpenPGPCrypter{armed: true, configured: true, pubKey: ring, secretKey: ring}
	return crypter
}

func MockDisarmedCrypter() (Crypter) {
	return &OpenPGPCrypter{armed: false, configured: true, pubKey: nil, secretKey: nil}
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

type ExternalGPGCrypter struct {
}

func (c *ExternalGPGCrypter) IsUsed() bool {
	return len(GetKeyRingId()) > 0
}

func (c *ExternalGPGCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	cmd := exec.Command("gpg", "-e", "-z", "0", "-r", GetKeyRingId())

	cmd.Stdout = writer
	pr, pw := io.Pipe()
	cmd.Stdin = pr
	go func() {
		err := cmd.Run()
		if err != nil {
			panic(err)
		}
	}()

	return pw, nil
}

func (c *ExternalGPGCrypter) Decrypt(reader io.ReadCloser) ([]byte, error) {
	cmd := exec.Command("gpg", "-d", "-q", "--batch")


	cmd.Stdin = reader

	return cmd.Output()
}

const waleGpgKey string = `
-----BEGIN PGP PRIVATE KEY BLOCK-----
Version: GnuPG v1

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
=DlRG
-----END PGP PRIVATE KEY BLOCK-----`

const waleWALfilename = "testdata/000000010000000000000024.lzo"

func TestDecryptWALElzo(t *testing.T) {
	crypter := mockCrypter(waleGpgKey)
	f, err := os.Open(waleWALfilename)
	if err != nil {
		t.Fatal(err)
	}
	decrypt, err := crypter.Decrypt(f)
	if err != nil {
		t.Fatal(err)
	}
	bytes1, err := ioutil.ReadAll(decrypt)
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(gpgBin, "--import")
	if err != nil {
		t.Fatal(err)
	}

	command.Stdin = strings.NewReader(waleGpgKey)
	if err != nil {
		t.Fatal(err)
	}
	_, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}

	os.Setenv("WALE_GPG_KEY_ID", "walg-server-test")
	defer os.Unsetenv("WALE_GPG_KEY_ID")

	ec := &ExternalGPGCrypter{}

	f, err = os.Open(waleWALfilename)
	if err != nil {
		t.Fatal(err)
	}
	bytes2, err := ec.Decrypt(f)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(bytes1, bytes2) {
		t.Log(bytes1)
		t.Log(bytes2)
		t.Fatal("Decryption result differ")
	}

	/*
	command = exec.Command(gpgBin, "--delete-secret-key", "--yes", "D32100BF1CDA62E5E50008F751EFFF0B6548E47F")
	_, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}*/
}
