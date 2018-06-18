package walg

import (
	"io"
	"hash"
	"crypto/md5"
	"encoding/hex"
)

type MD5Reader struct {
	internal io.Reader
	md5      hash.Hash
}

func newMd5Reader(reader io.Reader) *MD5Reader {
	return &MD5Reader{internal: reader, md5: md5.New()}
}

func (reader *MD5Reader) Read(p []byte) (n int, err error) {
	n, err = reader.internal.Read(p)
	if err != nil {
		return
	}
	_, err = reader.md5.Write(p[:n])
	return
}

func (reader *MD5Reader) Sum() string {
	bytes := reader.md5.Sum(nil)
	return hex.EncodeToString(bytes)
}
