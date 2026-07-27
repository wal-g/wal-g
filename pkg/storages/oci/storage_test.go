package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOCIStorageConfigures(t *testing.T) {
	t.Skip("Requires actual OCI credentials and token file")

	ociPrefix := "oci://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), ociPrefix,
		map[string]string{
			regionSetting:            "us-phoenix-1",
			tenancyOCIDSetting:       "ocid1.tenancy.oc1..test",
			securityTokenFileSetting: "/tmp/test-token",
		})

	assert.NoError(t, err)
}

func TestOCIStorageConfiguresWithOptionalSettings(t *testing.T) {
	t.Skip("Requires actual OCI credentials and token file")

	ociPrefix := "oci://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), ociPrefix,
		map[string]string{
			regionSetting:            "us-phoenix-1",
			tenancyOCIDSetting:       "ocid1.tenancy.oc1..test",
			securityTokenFileSetting: "/tmp/test-token",
			connectTimeoutSetting:    "60",
		})

	assert.NoError(t, err)
}

func TestOCIStorageRequiresRegion(t *testing.T) {
	ociPrefix := "oci://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), ociPrefix,
		map[string]string{
			tenancyOCIDSetting:       "ocid1.tenancy.oc1..test",
			securityTokenFileSetting: "/tmp/test-token",
		})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OCI_REGION is required")
}

func TestOCIStorageRequiresTenancyOCID(t *testing.T) {
	ociPrefix := "oci://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), ociPrefix,
		map[string]string{
			regionSetting:            "us-phoenix-1",
			securityTokenFileSetting: "/tmp/test-token",
		})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OCI_TENANCY_OCID is required")
}

func TestOCIStorageRequiresSecurityTokenFile(t *testing.T) {
	ociPrefix := "oci://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), ociPrefix,
		map[string]string{
			regionSetting:      "us-phoenix-1",
			tenancyOCIDSetting: "ocid1.tenancy.oc1..test",
		})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no authentication method configured")
}

func TestOCIStorageWithConfigFile(t *testing.T) {
	t.Skip("Requires OCI config file")

	ociPrefix := "oci://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), ociPrefix,
		map[string]string{
			regionSetting:      "us-phoenix-1",
			tenancyOCIDSetting: "ocid1.tenancy.oc1..test",
			configFileSetting:  "~/.oci/config",
			profileSetting:     "DEFAULT",
		})

	assert.NoError(t, err)
}

func TestOCIFolder(t *testing.T) {
	t.Skip("Credentials and OCI environment needed to run OCI tests")

	ociPrefix := "oci://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(t.Context(), ociPrefix,
		map[string]string{
			regionSetting:            "us-phoenix-1",
			tenancyOCIDSetting:       "ocid1.tenancy.oc1..xxxxx",
			securityTokenFileSetting: "/var/run/secrets/oci/token",
		})
	assert.NoError(t, err)

	// This would run the standard folder test suite
	// storage.RunFolderTest(st.RootFolder(), t)
	_ = st
}
