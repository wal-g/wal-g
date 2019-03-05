package swift

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"os"
	"testing"
)

func TestSwiftFolder(t *testing.T) {
	t.Skip("Credentials needed to run Swift Storage tests")

	os.Setenv("OS_USERNAME", "")
	os.Setenv("OS_PASSWORD", "")
	os.Setenv("OS_AUTH_URL", "")
	os.Setenv("OS_TENANT_NAME", "")
	os.Setenv("OS_REGION_NAME", "")

	storageFolder, err := ConfigureFolder("swift://test-container/wal-g-test-folder/sub0", nil)

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
