package s3_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

// mockS3ClientVersioning is a mock S3 client that supports ListObjectVersionsPages
// for testing versioning-related functionality.
type mockS3ClientVersioning struct {
	s3iface.S3API
	versions      []*s3.ObjectVersion
	deleteMarkers []*s3.DeleteMarkerEntry
}

func (m *mockS3ClientVersioning) ListObjectVersionsPages(
	input *s3.ListObjectVersionsInput,
	fn func(*s3.ListObjectVersionsOutput, bool) bool,
) error {
	output := &s3.ListObjectVersionsOutput{
		Versions:      m.versions,
		DeleteMarkers: m.deleteMarkers,
	}
	fn(output, true)
	return nil
}

func (m *mockS3ClientVersioning) GetBucketVersioning(input *s3.GetBucketVersioningInput) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{
		Status: aws.String(s3.BucketVersioningStatusEnabled),
	}, nil
}

func TestListFolder_VersioningEnabled_ExcludesDeletedObjects(t *testing.T) {
	now := time.Now()

	// Create mock data:
	// - object1.txt: has a version (LATEST) - should be included
	// - object2.txt: has a delete marker (LATEST) - should be EXCLUDED
	// - object3.txt: has a version (LATEST) - should be included
	mockClient := &mockS3ClientVersioning{
		versions: []*s3.ObjectVersion{
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
		deleteMarkers: []*s3.DeleteMarkerEntry{
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

	// Create mock data:
	// - object1.txt: has multiple versions, none deleted - all should be included
	mockClient := &mockS3ClientVersioning{
		versions: []*s3.ObjectVersion{
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
		deleteMarkers: []*s3.DeleteMarkerEntry{},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	folder := walgs3.NewFolder(mockClient, nil, "", config)

	objects, _, err := folder.ListFolder()

	require.NoError(t, err)

	// Should have 2 versions of object1.txt
	assert.Len(t, objects, 2)

	for _, obj := range objects {
		assert.Equal(t, "object1.txt", obj.GetName())
	}
}

func TestListFolder_VersioningEnabled_ExcludesAllVersionsOfDeletedObject(t *testing.T) {
	now := time.Now()

	// Create mock data:
	// - object1.txt: has multiple versions BUT also has a delete marker as LATEST
	//   All versions should be excluded
	mockClient := &mockS3ClientVersioning{
		versions: []*s3.ObjectVersion{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1-before-delete"),
				IsLatest:     aws.Bool(false), // Not latest because delete marker is latest
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
		deleteMarkers: []*s3.DeleteMarkerEntry{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("dm1"),
				IsLatest:     aws.Bool(true), // Delete marker is LATEST
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

	// Should have 0 objects - all versions of object1.txt should be excluded
	// because the LATEST version is a delete marker
	assert.Len(t, objects, 0, "All versions of a deleted object should be excluded")
}

func TestListFolder_VersioningEnabled_HandlesOldDeleteMarkers(t *testing.T) {
	now := time.Now()

	// Create mock data:
	// - object1.txt: was deleted (old delete marker) but then recreated (new version is LATEST)
	//   The object should be included
	mockClient := &mockS3ClientVersioning{
		versions: []*s3.ObjectVersion{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("v1-recreated"),
				IsLatest:     aws.Bool(true), // Object was recreated, this is LATEST
				LastModified: aws.Time(now),
				Size:         aws.Int64(100),
			},
		},
		deleteMarkers: []*s3.DeleteMarkerEntry{
			{
				Key:          aws.String("object1.txt"),
				VersionId:    aws.String("dm1-old"),
				IsLatest:     aws.Bool(false), // Old delete marker, not LATEST
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

	// Should have 1 object - the recreated version
	assert.Len(t, objects, 1)
	assert.Equal(t, "object1.txt", objects[0].GetName())
}

func TestListFolder_VersioningEnabled_ShowAllVersionsIncludesDeleted(t *testing.T) {
	now := time.Now()

	// Create mock data:
	// - object1.txt: has a version (LATEST) - should be included
	// - object2.txt: has a delete marker (LATEST) - normally excluded, but with ShowAllVersions should be included
	mockClient := &mockS3ClientVersioning{
		versions: []*s3.ObjectVersion{
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
				IsLatest:     aws.Bool(false), // Old version before deletion
				LastModified: aws.Time(now.Add(-time.Hour)),
				Size:         aws.Int64(200),
			},
		},
		deleteMarkers: []*s3.DeleteMarkerEntry{
			{
				Key:          aws.String("object2.txt"),
				VersionId:    aws.String("dm2"),
				IsLatest:     aws.Bool(true), // Object is deleted
				LastModified: aws.Time(now),
			},
		},
	}

	config := &walgs3.Config{
		Bucket:           "test-bucket",
		EnableVersioning: "enabled",
	}
	folder := walgs3.NewFolder(mockClient, nil, "", config)

	// Enable show all versions
	folder.SetShowAllVersions(true)

	objects, _, err := folder.ListFolder()

	require.NoError(t, err)

	// Should have 3 entries:
	// - object1.txt (version)
	// - object2.txt (old version)
	// - object2.txt (delete marker)
	assert.Len(t, objects, 3, "Should include all versions and delete markers")

	// Check that delete marker is included with DELETE info
	foundDeleteMarker := false
	for _, obj := range objects {
		info := obj.GetAdditionalInfo()
		if obj.GetName() == "object2.txt" && (info == "dm2 LATEST DELETE" || info == "dm2 DELETE") {
			foundDeleteMarker = true
			break
		}
	}
	assert.True(t, foundDeleteMarker, "Delete marker should be included with DELETE label")
}
