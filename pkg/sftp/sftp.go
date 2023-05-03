package sftp

import (
	"fmt"
	"os"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SSHRequisites using to decrease passed params
type SSHRequisites struct {
	Host string
	Port string

	Username       string
	Password       string
	PrivateKeyPath string
}

func NewSftpClient(requisites SSHRequisites) (*sftp.Client, error) {
	if requisites.Port == "" {
		requisites.Port = "22"
	}

	authMethods := make([]ssh.AuthMethod, 0)
	if requisites.PrivateKeyPath != "" {
		pkey, err := os.ReadFile(requisites.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read private key: %s", err)
		}

		signer, err := ssh.ParsePrivateKey(pkey)
		if err != nil {
			return nil, fmt.Errorf("unable to parse private key: %s", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if requisites.Password != "" {
		authMethods = append(authMethods, ssh.Password(requisites.Password))
	}

	config := &ssh.ClientConfig{
		User:            requisites.Username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	address := fmt.Sprint(requisites.Host, ":", requisites.Port)
	sshClient, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect via ssh by address %s: %s", address, err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, fmt.Errorf("failed to connect via sftp by address %s: %s", address, err)
	}

	return sftpClient, nil
}
