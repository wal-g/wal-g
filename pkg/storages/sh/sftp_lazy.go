package sh

import (
	"fmt"
	"os"
	"sync"

	"github.com/pkg/sftp"
	"github.com/wal-g/tracelog"
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
	Getwd() (string, error)
	Close() error
}

type SFTPLazy struct {
	address   string
	config    *ssh.ClientConfig
	client    SFTPClient
	clientMux *sync.Mutex
	connErr   error
}

func NewSFTPLazy(addr string, config *ssh.ClientConfig) *SFTPLazy {
	return &SFTPLazy{
		address:   addr,
		config:    config,
		clientMux: new(sync.Mutex),
	}
}

func (l *SFTPLazy) Client() (SFTPClient, error) {
	l.clientMux.Lock()
	defer l.clientMux.Unlock()

	needConnect := func() bool {
		// Establish the SFTP connection only once on the first call, and reuse the connection in all subsequent calls
		if l.client == nil {
			tracelog.DebugLogger.Printf("Establish an initial SFTP connection")
			return true
		}
		// Re-establish the SFTP connection if the previous one has died
		_, err := l.client.Getwd()
		if err != nil {
			tracelog.WarningLogger.Printf("Establish a new SFTP connection because the previous one has died with the error: getwd: %v", err)
			return true
		}
		return false
	}

	if needConnect() {
		client, err := connect(l.address, l.config)
		if err != nil {
			l.connErr = fmt.Errorf("lazy SSH connection error: %w", err)
		}
		l.client = client
	}

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
