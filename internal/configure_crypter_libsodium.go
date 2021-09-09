// +build libsodium

package internal

import (
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/crypto/libsodium"
)

func configureLibsodiumCrypter() crypto.Crypter {
	if viper.IsSet(LibsodiumKeySetting) {
		return libsodium.CrypterFromKey(viper.GetString(LibsodiumKeySetting), viper.GetString(LibsodiumKeyTransform))
	}

	if viper.IsSet(LibsodiumKeyPathSetting) {
		return libsodium.CrypterFromKeyPath(viper.GetString(LibsodiumKeyPathSetting), viper.GetString(LibsodiumKeyTransform))
	}

	return nil
}
