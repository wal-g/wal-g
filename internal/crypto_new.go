package internal

import (
	"bytes"
	"golang.org/x/crypto/openpgp"
	"io"
	"io/ioutil"
)

func GetKey(path string) io.Reader {
	byteData, err := ioutil.ReadFile(path)

	if err != nil {
		panic(err)
	}

	return bytes.NewReader(byteData)
}

func GetPGPKey(path string) (openpgp.EntityList, error) {
	gpgKeyReader := GetKey(path)

	entityList, err := openpgp.ReadArmoredKeyRing(gpgKeyReader)

	if err != nil {
		return nil, err
	}

	return entityList, nil
}
