package s3_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type listingWarningClient struct {
	mockS3ClientVersioning
	calls     int
	failPage  int
	errorCode string
	emptyPage bool
}

func (c *listingWarningClient) nextPage() error {
	c.calls++
	if c.calls == c.failPage {
		return &smithy.GenericAPIError{Code: c.errorCode, Message: "listing failed"}
	}
	return nil
}

func (c *listingWarningClient) ListObjectsV2(context.Context, *awss3.ListObjectsV2Input,
	...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	if err := c.nextPage(); err != nil {
		return nil, err
	}
	out := &awss3.ListObjectsV2Output{
		IsTruncated: aws.Bool(c.calls == 1), NextContinuationToken: aws.String("next"),
	}
	if !c.emptyPage {
		out.Contents = []types.Object{{
			Key:          aws.String(fmt.Sprintf("archive/wal_005/0000000800000002%08d.lz4", c.calls)),
			LastModified: aws.Time(time.Unix(0, 0)), Size: aws.Int64(123),
		}}
	}
	return out, nil
}

func (c *listingWarningClient) ListObjectVersions(context.Context, *awss3.ListObjectVersionsInput,
	...func(*awss3.Options)) (*awss3.ListObjectVersionsOutput, error) {
	if err := c.nextPage(); err != nil {
		return nil, err
	}
	out := &awss3.ListObjectVersionsOutput{
		IsTruncated: aws.Bool(c.calls == 1), NextKeyMarker: aws.String("next"), NextVersionIdMarker: aws.String("v1"),
	}
	if !c.emptyPage {
		out.Versions = []types.ObjectVersion{{
			Key:          aws.String(fmt.Sprintf("archive/wal_005/0000000800000002%08d.lz4", c.calls)),
			LastModified: aws.Time(time.Unix(0, 0)), Size: aws.Int64(123),
			VersionId: aws.String("v1"), IsLatest: aws.Bool(true),
		}}
	}
	return out, nil
}

func TestListingMissingPageWarning(t *testing.T) {
	for _, mode := range []string{"plain", "prefix", "versions"} {
		for _, tc := range []struct {
			name        string
			code        string
			failPage    int
			emptyPage   bool
			wantObjects int
			wantWarning bool
			wantError   bool
		}{
			{name: "success", wantObjects: 2},
			{name: "first missing key", code: "NoSuchKey", failPage: 1},
			{name: "first not found", code: "NotFound", failPage: 1},
			{name: "later missing key", code: "NoSuchKey", failPage: 2, wantObjects: 1, wantWarning: true},
			{name: "later not found", code: "NotFound", failPage: 2, wantObjects: 1, wantWarning: true},
			{name: "empty page then missing key", code: "NoSuchKey", failPage: 2, emptyPage: true, wantWarning: true},
			{name: "empty page then not found", code: "NotFound", failPage: 2, emptyPage: true, wantWarning: true},
			{name: "first denied", code: "AccessDenied", failPage: 1, wantError: true},
			{name: "later denied", code: "AccessDenied", failPage: 2, wantError: true},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				var warnings bytes.Buffer
				defer tracelog.WarningLogger.SetOutput(tracelog.WarningLogger.Writer())
				tracelog.WarningLogger.SetOutput(&warnings)
				client := &listingWarningClient{failPage: tc.failPage, errorCode: tc.code, emptyPage: tc.emptyPage}
				config := &s3.Config{Bucket: "bucket", EnableVersioning: "disabled"}
				if mode == "versions" {
					config.EnableVersioning = "enabled"
				}
				folder := s3.NewFolder(client, nil, "archive/wal_005/", config)
				var objects []storage.Object
				var err error
				if mode == "prefix" {
					objects, err = folder.ListObjectsWithPrefix(context.Background(), "0000000800000002")
				} else {
					objects, _, err = folder.ListFolder(context.Background())
				}
				if tc.wantError {
					require.ErrorContains(t, err, "AccessDenied")
				} else {
					require.NoError(t, err)
				}
				require.Len(t, objects, tc.wantObjects)
				for i, object := range objects {
					require.Equal(t, fmt.Sprintf("0000000800000002%08d.lz4", i+1), object.GetName())
				}
				if tc.wantWarning {
					require.Equal(t, 1, strings.Count(warnings.String(), "WARNING:"))
					require.Contains(t, warnings.String(), "S3 listing incomplete after successful pages")
					require.Contains(t, warnings.String(), "completed_pages=1")
					require.Contains(t, warnings.String(), tc.code)
				} else {
					require.Empty(t, warnings.String())
				}
				wantCalls := 2
				if tc.failPage == 1 {
					wantCalls = 1
				}
				require.Equal(t, wantCalls, client.calls)
			})
		}
	}
}
