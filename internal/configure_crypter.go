// +build !libsodium

package internal

import (
	"github.com/wal-g/wal-g/internal/crypto"
)

func configureLibsodiumCrypter() crypto.Crypter {
	return nil
}
