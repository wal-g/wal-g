// +build !libsodium

package internal

// This file contains functions that should return `nil`,
// in order to be able to build wal-g without specific implementations of the crypter.
// And the configure_crypter_<crypter>.go files must have a real implementation of the function.
//
// Thus, if the tag is missing, the condition:
// if crypter := configure<crypter>Crypter(); crypter != nil {
//     return crypter
// }
// will never be met.
// If there is a tag, we can configure the correct implementation of crypter.

import (
	"github.com/wal-g/wal-g/internal/crypto"
)

func configureLibsodiumCrypter() crypto.Crypter {
	return nil
}
