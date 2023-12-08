package storage

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Storage represents a storage of objects. It deals with resources shared by all folders, and provides the root folder
// as an entry point.
type Storage interface {
	// RootFolder of the storage. All objects that can be interacted with are located in it or its subfolders.
	RootFolder() Folder

	// Close releases all resources (files, connections, etc.) opened by the storage implementation, if any.
	Close() error
}

// HashableStorage is like Storage, but it supports hashing its config.
type HashableStorage interface {
	Storage

	// ConfigHash provides a hex-encoded hash of the config used to create a storage.
	ConfigHash() string
}

func ComputeConfigHash(storageType string, config any) (string, error) {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("serialize config: %w", err)
	}
	hash := md5.New()
	hash.Write([]byte(storageType))
	hash.Write(configBytes)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// WrapRootFolder allows to modify the storage root folder somehow (cd into a subfolder, wrap with some limiters, etc).
type WrapRootFolder func(prevFolder Folder) (newFolder Folder)
