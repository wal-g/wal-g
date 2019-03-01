package internal

import (
	"bytes"
	"golang.org/x/crypto/openpgp"
	"io"
	"io/ioutil"
)

func ReadKey(path string) (io.Reader, error) {
	byteData, err := ioutil.ReadFile(path)

	if err != nil {
		return nil, err
	}

	return bytes.NewReader(byteData), nil
}

func ReadPGPKey(path string) (openpgp.EntityList, error) {
	gpgKeyReader, err := ReadKey(path)

	if err != nil {
		return nil, err
	}

	entityList, err := openpgp.ReadArmoredKeyRing(gpgKeyReader)

	if err != nil {
		return nil, err
	}

	return entityList, nil
}

func DecryptSecretKey(entityList openpgp.EntityList, passphrase string) error {
	passphraseBytes := []byte(passphrase)

	for _, entity := range entityList {
		err := entity.PrivateKey.Decrypt(passphraseBytes)

		if err != nil {
			return err
		}

		for _, subKey := range entity.Subkeys {
			err := subKey.PrivateKey.Decrypt(passphraseBytes)

			if err != nil {
				return err
			}
		}
	}

	return nil
}
