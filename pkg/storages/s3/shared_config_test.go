package s3

import (
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func isolateAWSEnv(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, name := range []string{
		"AWS_CONFIG_FILE", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SESSION_TOKEN",
	} {
		t.Setenv(name, "")
	}
}

func writeAWSConfig(t *testing.T, accessKey string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config")
	content := "[default]\naws_access_key_id = " + accessKey + "\naws_secret_access_key = secret\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	return path
}

func configureTestStorage(t *testing.T, settings map[string]string) *Folder {
	t.Helper()
	all := map[string]string{regionSetting: "us-east-1", uploadConcurrencySetting: "1"}
	maps.Copy(all, settings)
	st, err := ConfigureStorage(t.Context(), "s3://bucket/prefix", all)
	require.NoError(t, err)
	return st.RootFolder().(*Folder)
}

func accessKey(t *testing.T, folder *Folder) string {
	t.Helper()
	client, ok := folder.s3API.(*s3.Client)
	require.True(t, ok)
	creds, err := client.Options().Credentials.Retrieve(t.Context())
	require.NoError(t, err)
	return creds.AccessKeyID
}

func TestSharedConfigFileSetting(t *testing.T) {
	isolateAWSEnv(t)
	path := writeAWSConfig(t, "storage-key")

	folder := configureTestStorage(t, map[string]string{sharedConfigFileSetting: path})
	require.Equal(t, path, folder.config.SharedConfigFile)
	require.Equal(t, "storage-key", accessKey(t, folder))
}

func TestSharedConfigFilePerStorage(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_CONFIG_FILE", writeAWSConfig(t, "env-key"))

	from := configureTestStorage(t, map[string]string{sharedConfigFileSetting: writeAWSConfig(t, "from-key")})
	to := configureTestStorage(t, map[string]string{sharedConfigFileSetting: writeAWSConfig(t, "to-key")})
	unset := configureTestStorage(t, nil)

	require.Equal(t, "from-key", accessKey(t, from))
	require.Equal(t, "to-key", accessKey(t, to))
	require.Equal(t, "env-key", accessKey(t, unset))
}
