package oci

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestPrivateKey generates a valid RSA private key for testing
func generateTestPrivateKey() ([]byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	})
	return privateKeyPEM, nil
}

func TestSecurityTokenConfigProviderValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError string
	}{
		{
			name: "missing security token file",
			config: &Config{
				Region:            "us-phoenix-1",
				TenancyOCID:       "ocid1.tenancy.oc1..test",
				SecurityTokenFile: "",
			},
			expectError: "OCI_SECURITY_TOKEN_FILE is required",
		},
		{
			name: "missing tenancy OCID",
			config: &Config{
				Region:            "us-phoenix-1",
				TenancyOCID:       "",
				SecurityTokenFile: "/tmp/token",
			},
			expectError: "OCI_TENANCY_OCID is required",
		},
		{
			name: "missing region",
			config: &Config{
				Region:            "",
				TenancyOCID:       "ocid1.tenancy.oc1..test",
				SecurityTokenFile: "/tmp/token",
			},
			expectError: "OCI_REGION is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newSecurityTokenConfigProvider(tt.config)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectError)
		})
	}
}

func TestSecurityTokenConfigProviderCreatesSuccessfully(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "token")
	keyPath := filepath.Join(tmpDir, "key.pem")
	err := os.WriteFile(tokenPath, []byte("test-token"), 0600)
	require.NoError(t, err)
	testKey, err := generateTestPrivateKey()
	require.NoError(t, err)
	err = os.WriteFile(keyPath, testKey, 0600)
	require.NoError(t, err)

	config := &Config{
		Region:            "us-phoenix-1",
		TenancyOCID:       "ocid1.tenancy.oc1..test",
		SecurityTokenFile: tokenPath,
		PrivateKeyFile:    keyPath,
	}

	provider, err := newSecurityTokenConfigProvider(config)
	require.NoError(t, err)
	assert.NotNil(t, provider)
	assert.Equal(t, "us-phoenix-1", provider.region)
	assert.Equal(t, "ocid1.tenancy.oc1..test", provider.tenancyOCID)
	assert.Equal(t, tokenPath, provider.tokenFile)
	assert.Equal(t, keyPath, provider.privateKeyFile)
}

func TestSecurityTokenConfigProviderReadToken(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "token")
	keyPath := filepath.Join(tmpDir, "key.pem")
	tokenContent := "test-oci-security-token"

	err := os.WriteFile(tokenPath, []byte(tokenContent), 0600)
	require.NoError(t, err)
	testKey, err := generateTestPrivateKey()
	require.NoError(t, err)
	err = os.WriteFile(keyPath, testKey, 0600)
	require.NoError(t, err)

	config := &Config{
		Region:            "us-phoenix-1",
		TenancyOCID:       "ocid1.tenancy.oc1..test",
		SecurityTokenFile: tokenPath,
		PrivateKeyFile:    keyPath,
	}

	provider, err := newSecurityTokenConfigProvider(config)
	require.NoError(t, err)

	keyID, err := provider.KeyID()
	require.NoError(t, err)
	assert.Equal(t, "ST$"+tokenContent, keyID)
}

func TestSecurityTokenConfigProviderReadTokenTrimsWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "token")
	keyPath := filepath.Join(tmpDir, "key.pem")
	tokenContent := "  test-oci-security-token\n\t"

	err := os.WriteFile(tokenPath, []byte(tokenContent), 0600)
	require.NoError(t, err)
	testKey, err := generateTestPrivateKey()
	require.NoError(t, err)
	err = os.WriteFile(keyPath, testKey, 0600)
	require.NoError(t, err)

	config := &Config{
		Region:            "us-phoenix-1",
		TenancyOCID:       "ocid1.tenancy.oc1..test",
		SecurityTokenFile: tokenPath,
		PrivateKeyFile:    keyPath,
	}

	provider, err := newSecurityTokenConfigProvider(config)
	require.NoError(t, err)

	keyID, err := provider.KeyID()
	require.NoError(t, err)
	assert.Equal(t, "ST$test-oci-security-token", keyID)
}

func TestSecurityTokenConfigProviderEmptyTokenFile(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "token")
	keyPath := filepath.Join(tmpDir, "key.pem")
	err := os.WriteFile(tokenPath, []byte("   \n\t  "), 0600)
	require.NoError(t, err)
	err = os.WriteFile(keyPath, []byte("test-key"), 0600)
	require.NoError(t, err)

	config := &Config{
		Region:            "us-phoenix-1",
		TenancyOCID:       "ocid1.tenancy.oc1..test",
		SecurityTokenFile: tokenPath,
		PrivateKeyFile:    keyPath,
	}

	provider, err := newSecurityTokenConfigProvider(config)
	require.NoError(t, err)

	_, err = provider.KeyID()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is empty")
}

func TestSecurityTokenConfigProviderInterfaceMethods(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "token")
	keyPath := filepath.Join(tmpDir, "key.pem")
	err := os.WriteFile(tokenPath, []byte("test-token"), 0600)
	require.NoError(t, err)
	err = os.WriteFile(keyPath, []byte("test-key"), 0600)
	require.NoError(t, err)

	config := &Config{
		Region:            "us-phoenix-1",
		TenancyOCID:       "ocid1.tenancy.oc1..test",
		SecurityTokenFile: tokenPath,
		PrivateKeyFile:    keyPath,
	}

	provider, err := newSecurityTokenConfigProvider(config)
	require.NoError(t, err)

	tenancy, err := provider.TenancyOCID()
	assert.NoError(t, err)
	assert.Equal(t, "ocid1.tenancy.oc1..test", tenancy)

	region, err := provider.Region()
	assert.NoError(t, err)
	assert.Equal(t, "us-phoenix-1", region)

	userOCID, err := provider.UserOCID()
	assert.NoError(t, err)
	assert.Empty(t, userOCID)

	fingerprint, err := provider.KeyFingerprint()
	assert.NoError(t, err)
	assert.Empty(t, fingerprint)

	_, err = provider.PrivateRSAKey()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode PEM block")

	authConfig, err := provider.AuthType()
	assert.NoError(t, err)
	assert.NotNil(t, authConfig)
}
