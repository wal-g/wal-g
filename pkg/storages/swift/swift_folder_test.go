package swift

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TestSwiftFolderUsingEnvVariables requires some OS_* env vars like OS_AUTH_URL or OS_PASSWORD.
// Different vars are required for various auth schemes. Please consult your provider.
// v1 and v2 example: https://github.com/ncw/swift/blob/b37a86bc3491c732a2b5ea198bc7cb307e239992/integration_test.sh
func TestSwiftFolderUsingEnvVariables(t *testing.T) {
	if os.Getenv("PG_TEST_STORAGE") != "swift" {
		t.Skip("Credentials needed to run Swift Storage tests")
	}

	st := time.Now()
	waitSwiftStartup()
	t.Logf("Waited %s for Swift container startup", time.Now().Sub(st).String())
	container := createTestContainerMust()
	t.Logf("Swift created test container: '%s'", container)

	storageFolderUsingEnvVars, err := ConfigureFolder(
		fmt.Sprintf("swift://%s/test-folder/sub0", container),
		nil,
	)
	assert.NoError(t, err)
	if t.Failed() {
		return
	}
	storage.RunFolderTest(storageFolderUsingEnvVars, t)
}

// createTestContainerMust creates a container with random name for test purposes.
// It uses v1 auth scheme. Algorithm is taken from
// https://github.com/NVIDIA/docker-swift/blob/25fd53f27217ed2bd16c6317cc0dcc473c1600f0/demo.sh
func createTestContainerMust() string {
	if os.Getenv("OS_AUTH_URL") == "" ||
		os.Getenv("OS_USERNAME") == "" ||
		os.Getenv("OS_PASSWORD") == "" {
		panic("Please provide OS_* env to work with OpenStack Swift")
	}
	name := fmt.Sprintf("test-container-%x", rand.Int63())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// curl -D- "$URL/auth/v1.0" -H "X-Auth-User: test:tester" -H "X-Auth-Key: testing"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, os.Getenv("OS_AUTH_URL"), nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("X-Auth-User", os.Getenv("OS_USERNAME"))
	req.Header.Set("X-Auth-Key", os.Getenv("OS_PASSWORD"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != http.StatusOK {
		panic(resp.Status)
	}
	token := resp.Header.Get("X-Auth-Token")
	storageURL := resp.Header.Get("X-Storage-Url")
	_ = resp.Body.Close()

	// curl -X PUT -i -H "X-Auth-Token: $TOKEN" $STORAGE_URL/test-container
	req, err = http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/%s", storageURL, name), nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("X-Auth-Token", token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != http.StatusCreated {
		panic(resp.Status)
	}
	_ = resp.Body.Close()

	return name
}

// waitSwiftStartup wait for valid HTTP answer from Swift. Container needs about 10 second to become ready after start.
func waitSwiftStartup() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 0; i < 15; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, os.Getenv("OS_AUTH_URL"), nil)
		if err != nil {
			panic(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(time.Duration(i) * time.Second)
	}
}
