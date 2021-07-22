package sh

import (
	"io"
	"os"

	"github.com/pkg/sftp"
)

type SftpClient interface {
	Lock()
	Unlock()
	ReadDir(path string) ([]os.FileInfo, error)
	Join(elem ...string) string
	Remove(path string) error
	Stat(p string) (os.FileInfo, error)
	OpenFile(path string) (io.ReadCloser, error)
	CreateFile(path string) (*sftp.File, error)
	Mkdir(path string) error
}

type extendedSftpClient struct {
	*sftp.Client
}

func (client *extendedSftpClient) OpenFile(path string) (io.ReadCloser, error) {
	return client.Open(path)
}

func (client *extendedSftpClient) CreateFile(path string) (*sftp.File, error) {
	return client.Create(path)
}

func (client *extendedSftpClient) Mkdir(path string) error {
	return client.MkdirAll(path)
}

func extend(client *sftp.Client) *extendedSftpClient {
	return &extendedSftpClient{client}
}
