package s3_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

// mockS3ClientVersioning is a minimal walgs3.API used to drive ListObjectVersions
// in versioning tests. v2 paginators consume the API directly and call NextPage
// until IsTruncated is false; we leave IsTruncated nil so a single page returns.
type mockS3ClientVersioning struct {
	versions       []types.ObjectVersion
	deleteMarkers  []types.DeleteMarkerEntry
	commonPrefixes []types.CommonPrefix
}

var _ walgs3.API = (*mockS3ClientVersioning)(nil)

func (m *mockS3ClientVersioning) ListObjectVersions(_ context.Context, _ *s3.ListObjectVersionsInput,
	_ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	return &s3.ListObjectVersionsOutput{
		CommonPrefixes: m.commonPrefixes,
		Versions:       m.versions,
		DeleteMarkers:  m.deleteMarkers,
	}, nil
}

func (m *mockS3ClientVersioning) GetBucketVersioning(_ context.Context, _ *s3.GetBucketVersioningInput,
	_ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{
		Status: types.BucketVersioningStatusEnabled,
	}, nil
}

func (m *mockS3ClientVersioning) GetObject(_ context.Context, _ *s3.GetObjectInput,
	_ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{}, nil
}

func (m *mockS3ClientVersioning) HeadObject(_ context.Context, _ *s3.HeadObjectInput,
	_ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3ClientVersioning) CopyObject(_ context.Context, _ *s3.CopyObjectInput,
	_ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}

func (m *mockS3ClientVersioning) DeleteObjects(_ context.Context, _ *s3.DeleteObjectsInput,
	_ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{}, nil
}

func (m *mockS3ClientVersioning) ListObjects(_ context.Context, _ *s3.ListObjectsInput,
	_ ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	return &s3.ListObjectsOutput{}, nil
}

func (m *mockS3ClientVersioning) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input,
	_ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

func (m *mockS3ClientVersioning) GetBucketLocation(_ context.Context, _ *s3.GetBucketLocationInput,
	_ ...func(*s3.Options)) (*s3.GetBucketLocationOutput, error) {
	return &s3.GetBucketLocationOutput{}, nil
}

func TestListFolder_VersioningEnabled_ExcludesDeletedObjects(t *testing.T) {
	now := time.Now()

	// Create mock data:
	// - object1.txt: has a version (LATEST) - should be included
	// - object2.txt: has a delete marker (LATEST) - should be EXCLUDED
	// - object3.txt: has a version (LATEST) - should be included
	mockClient := &mockS3ClientVersioning{
		versions: []types.ObjectVersion{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
				Size:         aws.Int64(100),
			},
			{
				Key:          aws.String("object2.txt"),
				VersionId:    aws.String("v2-old"),
				IsLatest:     aws.Bool(false), // Old version, not latest
				LastModified: aws.Time(now.Add(-time.Hour)),
				Size:         aws.Int64(200),
			},
			{
				Key:          aws.String("object3.txt"),
				VersionId:    aws.String("v3"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
				Size:         aws.Int64(300),
			},
		},
		deleteMarkers: []types.DeleteMarkerEntry{
			{
				Key:          aws.String("object2.txt"),
				VersionId:    aws.String("dm2"),
				IsLatest:     aws.Bool(true), // This is the LATEST version - object is deleted
				LastModified: aws.Time(now),
			},
		},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	folder := walgs3.NewFolder(mockClient, nil, "", config)

	objects, subFolders, err := folder.ListFolder()

	require.NoError(t, err)
	assert.Empty(t, subFolders)

	// Should only have 2 objects (object1.txt and object3.txt)
	// object2.txt should be excluded because its LATEST version is a delete marker
	assert.Len(t, objects, 2)

	objectNames := make([]string, len(objects))
	for i, obj := range objects {
		objectNames[i] = obj.GetName()
	}

	assert.Contains(t, objectNames, "object1.txt")
	assert.Contains(t, objectNames, "object3.txt")
	assert.NotContains(t, objectNames, "object2.txt", "Deleted object should not be in the list")
}

func TestListFolder_VersioningEnabled_IncludesAllVersionsOfNonDeletedObjects(t *testing.T) {
	now := time.Now()

	mockClient := &mockS3ClientVersioning{
		versions: []types.ObjectVersion{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1-latest"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
				Size:         aws.Int64(100),
			},
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1-old"),
				IsLatest:     aws.Bool(false),
				LastModified: aws.Time(now.Add(-time.Hour)),
				Size:         aws.Int64(90),
			},
		},
		deleteMarkers: []types.DeleteMarkerEntry{},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	folder := walgs3.NewFolder(mockClient, nil, "", config)

	objects, _, err := folder.ListFolder()

	require.NoError(t, err)

	assert.Len(t, objects, 2)

	for _, obj := range objects {
		assert.Equal(t, "object1.txt", obj.GetName())
	}
}

func TestListFolder_VersioningEnabled_ExcludesAllVersionsOfDeletedObject(t *testing.T) {
	now := time.Now()

	mockClient := &mockS3ClientVersioning{
		versions: []types.ObjectVersion{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1-before-delete"),
				IsLatest:     aws.Bool(false),
				LastModified: aws.Time(now.Add(-time.Minute)),
				Size:         aws.Int64(100),
			},
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1-old"),
				IsLatest:     aws.Bool(false),
				LastModified: aws.Time(now.Add(-time.Hour)),
				Size:         aws.Int64(90),
			},
		},
		deleteMarkers: []types.DeleteMarkerEntry{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("dm1"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
			},
		},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	folder := walgs3.NewFolder(mockClient, nil, "", config)

	objects, _, err := folder.ListFolder()

	require.NoError(t, err)

	assert.Len(t, objects, 0, "All versions of a deleted object should be excluded")
}

func TestListFolder_VersioningEnabled_HandlesOldDeleteMarkers(t *testing.T) {
	now := time.Now()

	mockClient := &mockS3ClientVersioning{
		versions: []types.ObjectVersion{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1-recreated"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
				Size:         aws.Int64(100),
			},
		},
		deleteMarkers: []types.DeleteMarkerEntry{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("dm1-old"),
				IsLatest:     aws.Bool(false),
				LastModified: aws.Time(now.Add(-time.Hour)),
			},
		},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	folder := walgs3.NewFolder(mockClient, nil, "", config)

	objects, _, err := folder.ListFolder()

	require.NoError(t, err)

	assert.Len(t, objects, 1)
	assert.Equal(t, "object1.txt", objects[0].GetName())
}

func TestListFolder_VersioningEnabled_ShowAllVersionsIncludesDeleted(t *testing.T) {
	now := time.Now()

	mockClient := &mockS3ClientVersioning{
		versions: []types.ObjectVersion{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
				Size:         aws.Int64(100),
			},
			{
				Key:          aws.String("object2.txt"),
				VersionId:    aws.String("v2-old"),
				IsLatest:     aws.Bool(false),
				LastModified: aws.Time(now.Add(-time.Hour)),
				Size:         aws.Int64(200),
			},
		},
		deleteMarkers: []types.DeleteMarkerEntry{
			{
				Key:          aws.String("object2.txt"),
				VersionId:    aws.String("dm2"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
			},
		},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	folder := walgs3.NewFolder(mockClient, nil, "", config)

	folder.SetShowAllVersions(true)

	objects, _, err := folder.ListFolder()

	require.NoError(t, err)

	assert.Len(t, objects, 3, "Should include all versions and delete markers")

	foundDeleteMarker := false
	for _, obj := range objects {
		info := obj.GetAdditionalInfo()
		version := obj.GetVersionID()
		if obj.GetName() == "object2.txt" && version == "dm2" && (info == "LATEST DELETE" || info == "DELETE") {
			foundDeleteMarker = true
			break
		}
	}
	assert.True(t, foundDeleteMarker, "Delete marker should be included with DELETE label")
}

func TestListFolder_VersioningEnabled_ShowAllVersionsPropagatesToSubfoldersFromListing(t *testing.T) {
	now := time.Now()

	mockClient := &mockS3ClientVersioning{
		commonPrefixes: []types.CommonPrefix{
			{Prefix: aws.String("dir/")},
		},
		versions: []types.ObjectVersion{
			{
				Key:          aws.String("dir/object2.txt"),
				VersionId:    aws.String("v2-old"),
				IsLatest:     aws.Bool(false),
				LastModified: aws.Time(now.Add(-time.Hour)),
				Size:         aws.Int64(200),
			},
		},
		deleteMarkers: []types.DeleteMarkerEntry{
			{
				Key:          aws.String("dir/object2.txt"),
				VersionId:    aws.String("dm2"),
				IsLatest:     aws.Bool(true),
				LastModified: aws.Time(now),
			},
		},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	root := walgs3.NewFolder(mockClient, nil, "", config)
	root.SetShowAllVersions(true)

	_, subFolders, err := root.ListFolder()
	require.NoError(t, err)
	require.Len(t, subFolders, 1)

	sub := subFolders[0]
	objects, _, err := sub.ListFolder()
	require.NoError(t, err)

	require.Len(t, objects, 2)

	foundDelete := false
	for _, obj := range objects {
		if obj.GetName() == "object2.txt" && obj.GetVersionID() == "dm2" && (obj.GetAdditionalInfo() == "LATEST DELETE" || obj.GetAdditionalInfo() == "DELETE") {
			foundDelete = true
		}
	}
	assert.True(t, foundDelete, "Delete marker in subfolder should be included when --all-versions is enabled")
}
