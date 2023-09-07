//go:build libsodium
// +build libsodium

package internal

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/libsodium"
)

func configureLibsodiumCrypter(config *viper.Viper) (crypto.Crypter, error) {
	if viper.IsSet(LibsodiumKeySetting) {
		return libsodium.CrypterFromKey(viper.GetString(LibsodiumKeySetting), viper.GetString(LibsodiumKeyTransform)), nil
	}

	if viper.IsSet(LibsodiumKeyPathSetting) {
		return libsodium.CrypterFromKeyPath(viper.GetString(LibsodiumKeyPathSetting), viper.GetString(LibsodiumKeyTransform)), nil
	}

	return nil, errors.New("there is no any supported libsodium crypter configuration")
}
