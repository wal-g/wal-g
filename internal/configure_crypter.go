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
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
)

func configureLibsodiumCrypter() crypto.Crypter {
	if viper.IsSet(LibsodiumKeySetting) {
		tracelog.ErrorLogger.Fatalf("non-empty WALG_LIBSODIUM_KEY but wal-g was not compiled with libsodium")
	}

	if viper.IsSet(LibsodiumKeyPathSetting) {
		tracelog.ErrorLogger.Fatalf("non-empty WALG_LIBSODIUM_KEY_PATH but wal-g was not compiled with libsodium")
	}

	return nil
}
