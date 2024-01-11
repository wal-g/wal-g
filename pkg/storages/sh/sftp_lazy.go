package sh

import (
	"fmt"
	"os"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPClient interface {
	Lock()
	Unlock()
	ReadDir(path string) ([]os.FileInfo, error)
	Join(elem ...string) string
	Remove(path string) error
	Stat(p string) (os.FileInfo, error)
	Open(path string) (*sftp.File, error)
	Create(path string) (*sftp.File, error)
	MkdirAll(path string) error
	Close() error
}

type SFTPLazy struct {
	address string
	config  *ssh.ClientConfig
	client  SFTPClient
	connErr error
	once    *sync.Once
}

func NewSFTPLazy(addr string, config *ssh.ClientConfig) *SFTPLazy {
	return &SFTPLazy{
		address: addr,
		config:  config,
		once:    new(sync.Once),
	}
}

func (l *SFTPLazy) Client() (SFTPClient, error) {
	// Establish the SFTP connection only once on the first call, and reuse the connection in all subsequent calls
	l.once.Do(func() {
		client, err := connect(l.address, l.config)
		if err != nil {
			l.connErr = fmt.Errorf("lazy SSH connection error: %w", err)
		}
		l.client = client
	})
	return l.client, l.connErr
}

func connect(addr string, config *ssh.ClientConfig) (*sftp.Client, error) {
	sshClient, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s via SSH: %w", addr, err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s via SFTP: %w", addr, err)
	}

	return sftpClient, nil
}
