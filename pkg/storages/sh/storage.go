package sh

import (
	"fmt"
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/crypto/ssh"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	sftpClientLazy *SFTPLazy
	rootFolder     storage.Folder
	hash           string
}

type Config struct {
	Secrets        *Secrets `json:"-"`
	Host           string
	Port           string
	RootPath       string
	User           string
	PrivateKeyPath string
}

type Secrets struct {
	Password string
}

// TODO: Unit tests
func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	var authMethods []ssh.AuthMethod
	if config.PrivateKeyPath != "" {
		pkey, err := os.ReadFile(config.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("read SSH private key: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(pkey)
		if err != nil {
			return nil, fmt.Errorf("parse SSH private key: %w", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}
	if config.Secrets.Password != "" {
		authMethods = append(authMethods, ssh.Password(config.Secrets.Password))
	}

	sshConfig := &ssh.ClientConfig{
		User: config.User,
		Auth: authMethods,
		// TODO: Allow checking SSH host keys if it's needed
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	address := fmt.Sprint(config.Host, ":", config.Port)
	client := NewSFTPLazy(address, sshConfig)

	path := storage.AddDelimiterToPath(config.RootPath)
	var folder storage.Folder = NewFolder(client, path)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	hash, err := storage.ComputeConfigHash("sh", config)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{client, folder, hash}, nil
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) ConfigHash() string {
	return s.hash
}

func (s *Storage) Close() error {
	client, connErr := s.sftpClientLazy.Client()
	// Don't try to close the client if the initial connection failed
	if connErr != nil {
		tracelog.DebugLogger.Printf("SSH storage isn't closed due to the initial connection error: %v", connErr)
		return nil
	}
	err := client.Close()
	if err != nil {
		return fmt.Errorf("close SFTP client: %w", err)
	}
	tracelog.DebugLogger.Printf("SSH storage closed")
	return nil
}
