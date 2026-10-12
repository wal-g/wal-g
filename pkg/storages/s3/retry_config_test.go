package s3

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

func TestWalgRetrySettingsOverrideAWSConfig(t *testing.T) {
	shared := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(shared, []byte("[profile test-retries]\nregion = us-east-1\nmax_attempts = 9\nretry_mode = adaptive\n"), 0600))
	t.Setenv("AWS_CONFIG_FILE", shared)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("AWS_PROFILE", "test-retries")
	for _, envAttempts := range []string{"", "7"} {
		for _, retries := range []int{0, 2, defaultMaxRetries} {
			t.Run(fmt.Sprintf("env=%s/retries=%d", envAttempts, retries), func(t *testing.T) {
				t.Setenv("AWS_MAX_ATTEMPTS", envAttempts)
				cfg, opts, err := loadAWSConfig(t.Context(), &Config{Secrets: &Secrets{}, Region: "us-east-1",
					MaxRetries: retries, MaxThrottlingRetryDelay: time.Nanosecond})
				require.NoError(t, err)
				cfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
				attempts := 0
				cfg.HTTPClient = smithyhttp.ClientDoFunc(func(req *http.Request) (*http.Response, error) {
					attempts++
					return &http.Response{StatusCode: 500, Header: http.Header{}, Request: req,
						Body: io.NopCloser(strings.NewReader("<Error><Code>InternalError</Code></Error>"))}, nil
				})
				_, err = awss3.NewFromConfig(cfg, opts...).HeadObject(t.Context(), &awss3.HeadObjectInput{
					Bucket: aws.String("bucket"), Key: aws.String("object")})
				require.Error(t, err)
				require.Equal(t, retries+1, attempts)
			})
		}
	}
}

func TestThrottleDelayBounds(t *testing.T) {
	r := newRetryerFunc(&Config{MaxRetries: 2, MinThrottlingRetryDelay: 5 * time.Second, MaxThrottlingRetryDelay: 6 * time.Second})()
	for _, err := range []error{
		&smithy.GenericAPIError{Code: "SlowDown"},
		&smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 429}}, Err: fmt.Errorf("throttled")},
	} {
		for _, attempt := range []int{1, 10} {
			delay, delayErr := r.RetryDelay(attempt, err)
			require.NoError(t, delayErr)
			require.GreaterOrEqual(t, delay, 5*time.Second)
			require.LessOrEqual(t, delay, 6*time.Second)
		}
	}
}

func TestRejectInvalidRetrySettings(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{maxRetriesSetting, "-1"}, {maxRetriesSetting, strconv.Itoa(int(^uint(0) >> 1))},
		{maxRetriesSetting, "invalid"}, {rangeQueriesMaxRetriesSetting, "-1"},
		{minThrottlingRetryDelaySetting, "-1"}, {maxThrottlingRetryDelaySetting, "0"},
		{maxThrottlingRetryDelaySetting, "400"}, {maxThrottlingRetryDelaySetting, "9223372036854775807"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			settings := map[string]string{uploadConcurrencySetting: "1", tc.key: tc.value}
			_, err := ConfigureStorage(t.Context(), "s3://bucket/prefix", settings)
			require.Error(t, err)
		})
	}
}
