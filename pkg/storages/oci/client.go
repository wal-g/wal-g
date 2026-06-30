package oci

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

var errFileModifiedDuringRead = errors.New("file modified during read")

// securityTokenConfigProvider implements OCI authentication using a security token from a file.
// This is used when an external process (e.g., background service) manages token refresh.
type securityTokenConfigProvider struct {
	tenancyOCID      string
	region           string
	tokenFile        string
	privateKeyFile   string
	privateKey       *rsa.PrivateKey
	cachedToken      string
	cachedTokenMtime time.Time
	mu               sync.RWMutex
}

func newSecurityTokenConfigProvider(config *Config) (*securityTokenConfigProvider, error) {
	if config.TenancyOCID == "" {
		return nil, fmt.Errorf("OCI_TENANCY_OCID is required")
	}
	if config.Region == "" {
		return nil, fmt.Errorf("OCI_REGION is required")
	}
	if config.SecurityTokenFile == "" {
		return nil, fmt.Errorf("OCI_SECURITY_TOKEN_FILE is required")
	}
	if config.PrivateKeyFile == "" {
		return nil, fmt.Errorf("OCI_PRIVATE_KEY_FILE is required")
	}

	return &securityTokenConfigProvider{
		tenancyOCID:    config.TenancyOCID,
		region:         config.Region,
		tokenFile:      config.SecurityTokenFile,
		privateKeyFile: config.PrivateKeyFile,
	}, nil
}

func (p *securityTokenConfigProvider) TenancyOCID() (string, error) {
	return p.tenancyOCID, nil
}

func (p *securityTokenConfigProvider) UserOCID() (string, error) {
	return "", nil
}

func (p *securityTokenConfigProvider) KeyFingerprint() (string, error) {
	return "", nil
}

func (p *securityTokenConfigProvider) Region() (string, error) {
	return p.region, nil
}

func (p *securityTokenConfigProvider) KeyID() (string, error) {
	if err := p.loadIfStale(); err != nil {
		return "", err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return "ST$" + p.cachedToken, nil
}

func (p *securityTokenConfigProvider) PrivateRSAKey() (*rsa.PrivateKey, error) {
	if err := p.loadIfStale(); err != nil {
		return nil, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.privateKey, nil
}

// readFilesConsistently reads both files and verifies they weren't modified during the read.
// Returns the file contents and the initial token file mtime, or an error if files were modified.
func (p *securityTokenConfigProvider) readFilesConsistently() (tokenBytes []byte, keyBytes []byte, tokenMtime time.Time, err error) {
	// Stat both files
	tokenStatBefore, err := os.Stat(p.tokenFile)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("stat token file %s: %w", p.tokenFile, err)
	}
	keyStatBefore, err := os.Stat(p.privateKeyFile)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("stat private key file %s: %w", p.privateKeyFile, err)
	}

	// Read both files into memory
	tokenBytes, err = os.ReadFile(p.tokenFile)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("reading security token from %s: %w", p.tokenFile, err)
	}
	keyBytes, err = os.ReadFile(p.privateKeyFile)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("reading private key from %s: %w", p.privateKeyFile, err)
	}

	// Stat both files again and compare
	tokenStatAfter, err := os.Stat(p.tokenFile)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("re-stat token file %s: %w", p.tokenFile, err)
	}
	keyStatAfter, err := os.Stat(p.privateKeyFile)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("re-stat private key file %s: %w", p.privateKeyFile, err)
	}

	if !tokenStatBefore.ModTime().Equal(tokenStatAfter.ModTime()) {
		return nil, nil, time.Time{}, fmt.Errorf("%w: token file", errFileModifiedDuringRead)
	}
	if !keyStatBefore.ModTime().Equal(keyStatAfter.ModTime()) {
		return nil, nil, time.Time{}, fmt.Errorf("%w: private key file", errFileModifiedDuringRead)
	}

	return tokenBytes, keyBytes, tokenStatBefore.ModTime(), nil
}

// loadIfStale reloads both token and private key if the token file has been modified.
// This ensures the private key stays in sync with the token when they're rotated together.
func (p *securityTokenConfigProvider) loadIfStale() error {
	tokenStat, err := os.Stat(p.tokenFile)
	if err != nil {
		return fmt.Errorf("stat token file %s: %w", p.tokenFile, err)
	}

	// Fast path: check if cache is fresh
	p.mu.RLock()
	fresh := p.privateKey != nil && tokenStat.ModTime().Equal(p.cachedTokenMtime)
	p.mu.RUnlock()
	if fresh {
		return nil
	}

	// Slow path: reload both token and private key atomically
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if p.privateKey != nil && tokenStat.ModTime().Equal(p.cachedTokenMtime) {
		return nil
	}

	// Retry reading files if they're modified during read (rotation race)
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		tokenBytes, keyBytes, tokenMtime, err := p.readFilesConsistently()
		if err != nil {
			if errors.Is(err, errFileModifiedDuringRead) {
				lastErr = err
				continue
			}
			return err
		}

		// Parse token
		token := strings.TrimSpace(string(tokenBytes))
		if token == "" {
			return fmt.Errorf("security token file %s is empty", p.tokenFile)
		}

		// Parse PEM-encoded private key
		block, _ := pem.Decode(keyBytes)
		if block == nil {
			return fmt.Errorf("failed to decode PEM block from %s", p.privateKeyFile)
		}
		privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return fmt.Errorf("parsing private key from %s: %w", p.privateKeyFile, err)
		}

		// Atomically update all cached values
		p.cachedToken = token
		p.privateKey = privateKey
		p.cachedTokenMtime = tokenMtime
		return nil
	}

	return fmt.Errorf("files kept rotating during reads after %d attempts: %w", maxAttempts, lastErr)
}

func (p *securityTokenConfigProvider) AuthType() (common.AuthConfig, error) {
	return common.AuthConfig{
		AuthType:         common.UserPrincipal,
		IsFromConfigFile: false,
	}, nil
}

type ociClient struct {
	objectStorageClient *objectstorage.ObjectStorageClient
	namespace           string
	mu                  sync.RWMutex
}

// createTLSConfig creates a TLS configuration with custom CA certificates.
func createTLSConfig(caCertFile string) (*tls.Config, error) {
	caCertPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("loading system cert pool: %w", err)
	}
	if caCertPool == nil {
		caCertPool = x509.NewCertPool()
	}

	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate from %s: %w", caCertFile, err)
	}
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate from %s", caCertFile)
	}

	return &tls.Config{RootCAs: caCertPool}, nil
}

// configureClient creates an OCI object storage client using standard authentication methods.
func configureClient(config *Config) (*ociClient, error) {
	var configProvider common.ConfigurationProvider
	var err error

	// Try authentication methods in order of preference
	if config.SecurityTokenFile != "" {
		// Security token from file, for workload identity integrations.
		configProvider, err = newSecurityTokenConfigProvider(config)
		if err != nil {
			return nil, fmt.Errorf("creating security token config provider: %w", err)
		}
	} else if config.ConfigFile != "" {
		// Standard OCI config file (~/.oci/config)
		profile := config.Profile
		if profile == "" {
			profile = "DEFAULT"
		}
		configProvider = common.CustomProfileConfigProvider(config.ConfigFile, profile)
	} else {
		return nil, fmt.Errorf("no authentication method configured: set OCI_SECURITY_TOKEN_FILE or OCI_CONFIG_FILE")
	}

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, fmt.Errorf("creating OCI object storage client: %w", err)
	}

	client.SetRegion(config.Region)

	// Configure HTTP client with custom transport
	httpClient := &http.Client{}

	// Clone DefaultTransport to preserve production-tuned defaults (connection pooling, etc.)
	transport := http.DefaultTransport.(*http.Transport).Clone()

	if config.ConnectTimeout > 0 {
		transport.DialContext = (&net.Dialer{
			Timeout: time.Duration(config.ConnectTimeout) * time.Second,
		}).DialContext
	}

	if config.CACertFile != "" {
		tlsConfig, err := createTLSConfig(config.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("creating TLS config: %w", err)
		}
		transport.TLSClientConfig = tlsConfig
	}

	httpClient.Transport = transport
	client.HTTPClient = httpClient

	return &ociClient{
		objectStorageClient: &client,
	}, nil
}

// getNamespace returns the OCI namespace, fetching and caching it on first use.
func (c *ociClient) getNamespace(ctx context.Context) (string, error) {
	c.mu.RLock()
	if c.namespace != "" {
		ns := c.namespace
		c.mu.RUnlock()
		return ns, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.namespace != "" {
		return c.namespace, nil
	}

	namespaceReq := objectstorage.GetNamespaceRequest{}
	namespaceResp, err := c.objectStorageClient.GetNamespace(ctx, namespaceReq)
	if err != nil {
		return "", fmt.Errorf("getting OCI namespace: %w", err)
	}

	c.namespace = *namespaceResp.Value
	return c.namespace, nil
}
