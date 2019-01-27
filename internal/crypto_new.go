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

func GetPGPKey(path string) (openpgp.EntityList, error) {
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
