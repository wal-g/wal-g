package utility

import (
	"fmt"
	"os"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func NewSftpClient(host, port, user, password, privateKeyPath string) (*sftp.Client, error) {
	if port == "" {
		port = "22"
	}

	authMethods := make([]ssh.AuthMethod, 0)
	if privateKeyPath != "" {
		pkey, err := os.ReadFile(privateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to read private key: %s", err)
		}

		signer, err := ssh.ParsePrivateKey(pkey)
		if err != nil {
			return nil, fmt.Errorf("unable to parse private key: %s", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if password != "" {
		authMethods = append(authMethods, ssh.Password(password))
	}

	config := &ssh.ClientConfig{
		User:            user,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	address := fmt.Sprint(host, ":", port)
	sshClient, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect via ssh by address %s: %s", address, err)
	}
	defer sshClient.Close()

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, fmt.Errorf("failed to connect via sftp by address %s: %s", address, err)
	}

	return sftpClient, nil
}
