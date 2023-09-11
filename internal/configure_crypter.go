//go:build !libsodium
// +build !libsodium

package internal

// This file contains functions that should configure crypters,
// in order to be able to build wal-g without specific implementations of the crypter.
// And the configure_crypter_<crypter>.go files must have a real implementation of the function.
// If there is a tag, we can configure the correct implementation of crypter.

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/wal-g/wal-g/internal/crypto"
)

func configureLibsodiumCrypter(config *viper.Viper) (crypto.Crypter, error) {
	if config.IsSet(LibsodiumKeySetting) {
		return nil, errors.New("non-empty WALG_LIBSODIUM_KEY but wal-g was not compiled with libsodium")
	}

	if config.IsSet(LibsodiumKeyPathSetting) {
		return nil, errors.New("non-empty WALG_LIBSODIUM_KEY_PATH but wal-g was not compiled with libsodium")
	}

	return nil, errors.New("there is no any supported libsodium crypter configuration")
}
