package walg

import (
	"bytes"
	"encoding/json"
	"errors"
	"golang.org/x/crypto/openpgp"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
)

// Crypter is responsible for makeing cryptographical pipeline parts when needed
type Crypter interface {
	IsUsed() bool
	Encrypt(writer io.WriteCloser) (io.WriteCloser, error)
	Decrypt(reader io.ReadCloser) (io.Reader, error)
}

// OpenPGPCrypter incapsulates specific of cypher method
// Includes keys, infrastructutre information etc
// If many encryption methods will be used it worth
// to extract interface
type OpenPGPCrypter struct {
	configured, armed bool
	keyRingId         string

	pubKey    openpgp.EntityList
	secretKey openpgp.EntityList
}

// IsUsed is to check necessity of Crypter use
// Must be called prior to any other crypter call
func (crypter *OpenPGPCrypter) IsUsed() bool {
	if !crypter.configured {
		crypter.ConfigureGPGCrypter()
	}
	return crypter.armed
}

// ConfigureGPGCrypter is OpenPGPCrypter internal initialization
func (crypter *OpenPGPCrypter) ConfigureGPGCrypter() {
	crypter.configured = true
	crypter.keyRingId = GetKeyRingId()
	crypter.armed = len(crypter.keyRingId) != 0
}

// ErrCrypterUseMischief happens when crypter is used before initialization
var ErrCrypterUseMischief = errors.New("Crypter is not checked before use")

// Encrypt creates encryption writer from ordinary writer
func (crypter *OpenPGPCrypter) Encrypt(writer io.WriteCloser) (io.WriteCloser, error) {
	if !crypter.configured {
		return nil, ErrCrypterUseMischief
	}
	if crypter.pubKey == nil {
		armour, err := getPubRingArmour(crypter.keyRingId)
		if err != nil {
			return nil, err
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.pubKey = entitylist
	}

	return &DelayWriteCloser{writer, crypter.pubKey, nil}, nil
}

// DelayWriteCloser delays first writes.
// Encryption starts writing header immediately.
// But there is a lot of places where writer is instantiated long before pipe
// is ready. This is why here is used special writer, which delays encryption
// initialization before actual write. If no write occurs, initialization
// still is performed, to handle zero-byte files correctly
type DelayWriteCloser struct {
	inner io.WriteCloser
	el    openpgp.EntityList
	outer *io.WriteCloser
}

func (d *DelayWriteCloser) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if d.outer == nil {
		wc, err0 := openpgp.Encrypt(d.inner, d.el, nil, nil, nil)
		if err0 != nil {
			return 0, err
		}
		d.outer = &wc
	}
	n, err = (*d.outer).Write(p)
	return
}

// Close DelayWriteCloser
func (d *DelayWriteCloser) Close() error {
	if d.outer == nil {
		wc, err0 := openpgp.Encrypt(d.inner, d.el, nil, nil, nil)
		if err0 != nil {
			return err0
		}
		d.outer = &wc
	}

	return (*d.outer).Close()
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *OpenPGPCrypter) Decrypt(reader io.ReadCloser) (io.Reader, error) {
	if !crypter.configured {
		return nil, ErrCrypterUseMischief
	}
	if crypter.secretKey == nil {
		armour, err := getSecretRingArmour(crypter.keyRingId)
		if err != nil {
			return nil, err
		}

		entitylist, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armour))
		if err != nil {
			return nil, err
		}
		crypter.secretKey = entitylist
	}

	var md, err0 = openpgp.ReadMessage(reader, crypter.secretKey, nil, nil)
	if err0 != nil {
		return nil, err0
	}

	return md.UnverifiedBody, nil
}

// GetKeyRingId extracts name of a key to use from env variable
func GetKeyRingId() string {
	return os.Getenv("WALG_GPG_KEY_ID")
}

const gpgBin = "gpg"

// CachedKey is the data transfer object describing format of key ring cache
type CachedKey struct {
	KeyId string `json:"keyId"`
	Body  []byte `json:"body"`
}

// Here we read armoured version of Key by calling GPG process
func getPubRingArmour(keyId string) ([]byte, error) {
	var cache CachedKey
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, ".walg_key_cache")
		file, err := ioutil.ReadFile(cacheFilename)
		// here we ignore whatever error can occur
		if err == nil {
			json.Unmarshal(file, &cache)
			if cache.KeyId == keyId && len(cache.Body) > 0 { // don't return an empty cached value
				return cache.Body, nil
			}
		}
	}

	cmd := exec.Command(gpgBin, "-a", "--export", keyId)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	if stderr.Len() > 0 { // gpg -a --export <key-id> reports error on stderr and exits == 0 if the key isn't found
		return nil, errors.New(strings.TrimSpace(stderr.String()))
	}

	cache.KeyId = keyId
	cache.Body = out
	marshal, err := json.Marshal(&cache)
	if err == nil && len(cacheFilename) > 0 {
		ioutil.WriteFile(cacheFilename, marshal, 0644)
	}

	return out, nil
}

func getSecretRingArmour(keyId string) ([]byte, error) {
	out, err := exec.Command(gpgBin, "-a", "--export-secret-key", keyId).Output()
	if err != nil {
		return nil, err
	}
	return out, nil
}
