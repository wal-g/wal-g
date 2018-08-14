package walg_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"io/ioutil"
	"testing"
	"time"
)

var correctKeys = []string{"mockServer/base_backup/second.nop",
	"mockServer/base_backup/fourth.nop",
	"mockServer/base_backup/fifth.nop",
	"mockServer/base_backup/first.nop",
	"mockServer/base_backup/third.nop"}

func TestBackupErrors(t *testing.T) {
	folder := testtools.NewMockS3Folder(true, true)

	bk := walg.NewBackup(folder, "base_backupmockBackup")

	//CheckExistence error testing
	exists, _ := bk.CheckExistence()
	assert.Falsef(t, exists, "backup: expected mock backup to not exist")

	folder.S3API = testtools.NewMockS3Client(true, false)
	_, err := bk.CheckExistence()
	assert.Errorf(t, err, "backup: CheckExistence expected error but got '<nil>'")

	//GetLatestBackupKey error testing
	_, err = walg.GetLatestBackupKey(bk.Folder)
	assert.Errorf(t, err, "backup: expected error but got '<nil>'")

	//GetKeys error testing
	_, err = bk.GetKeys()
	assert.Errorf(t, err, "backup: expected error but got '<nil>'")

	//Test S3 ReaderMaker with error S3.
	keys := []string{"1.nop", "2.nop", "3.gzip", "4.lzo"}
	n := &testtools.NOPTarInterpreter{}

	out := make([]walg.ReaderMaker, len(keys))
	for i, key := range keys {
		s := walg.NewS3ReaderMaker(folder, aws.String(key), walg.GetFileExtension(key))
		out[i] = s
	}

	err = walg.ExtractAll(n, out)
	assert.Errorf(t, err, "backup: expected error but got '<nil>'")
}

// Tests backup-fetch methods including:
// GetLatestBackupKey()
// CheckExistence()
// GetKeys()
func TestBackup(t *testing.T) {
	folder := testtools.NewMockS3Folder(false, false)

	bk := walg.NewBackup(folder, "base_backupmockBackup")

	latest, _ := walg.GetLatestBackupKey(bk.Folder)
	assert.Equalf(t, "first.nop", latest, "backup: expected %s from 'GetLatestBackupKey' but got %s", "first", latest)

	exists, _ := bk.CheckExistence()
	assert.Truef(t, exists, "backup: expected mock backup to exist but 'CheckExistence' returned false")

	keys, err := bk.GetKeys()
	assert.NoErrorf(t, err, "backup: expected no error but got %+v\n", err)

	assert.Equal(t, correctKeys, keys)

	// Test S3 ReaderMaker
	n := &testtools.NOPTarInterpreter{}

	out := make([]walg.ReaderMaker, len(keys))
	for i, key := range keys {
		s := walg.NewS3ReaderMaker(folder, aws.String(key), walg.GetFileExtension(key))
		out[i] = s
		assert.Equalf(t, correctKeys[i], out[i].Path(), "backup: expected S3ReaderMaker key to be %s but got %s", correctKeys[i], out[i].Path())
	}

	err = walg.ExtractAll(n, out)
	assert.NoErrorf(t, err, "backup: could not extract from S3ReaderMaker")
}

func TestArchiveErrors(t *testing.T) {
	folder := testtools.NewMockS3Folder(true, true)

	arch := &walg.Archive{
		Folder:  folder,
		Archive: aws.String("mockArchive"),
	}

	// CheckExistence error testing
	exists, _ := arch.CheckExistence()
	assert.Falsef(t, exists, "archive: expected mock archive to not exist")

	folder.S3API = testtools.NewMockS3Client(true, false)
	_, err := arch.CheckExistence()
	assert.Errorf(t, err, "archive: CheckExistence expected error but got `<nil>`")

}

// Tests Archive functions including:
// CheckExistence()
// GetArchive()
func TestArchive(t *testing.T) {
	folder := testtools.NewMockS3Folder(false, false)

	arch := &walg.Archive{
		Folder:  folder,
		Archive: aws.String("mockArchive"),
	}

	exists, _ := arch.CheckExistence()
	assert.True(t, exists)

	body, err := arch.GetArchive()
	assert.NoError(t, err)

	allBody, err := ioutil.ReadAll(body)

	if err != nil {
		t.Log(err)
	}

	assert.Equal(t, "mock content", string(allBody[:]))

	folder.S3API = testtools.NewMockS3Client(true, false)

	_, err = arch.CheckExistence()
	assert.Error(t, err)

	_, err = arch.GetArchive()
	assert.Error(t, err)
}

func TestGetBackupTimeSlices(t *testing.T) {
	first := "mockServer/backup01" + walg.SentinelSuffix
	second := "mockServer/somedir/backup02" + walg.SentinelSuffix
	third := "mockServer/somedir/somesubdir/backup03" + walg.SentinelSuffix
	firstTime := time.Now().Add(time.Hour)
	secondTime := time.Now().Add(time.Minute)
	thirdTime := time.Now()

	c := []*s3.Object{
		{Key: &first, LastModified: &firstTime},
		{Key: &second, LastModified: &secondTime},
		{Key: &third, LastModified: &thirdTime},
	}
	objectsFromS3 := &s3.ListObjectsOutput{Contents: c}

	checkSortingPermutationResult(objectsFromS3, t) //123
	c[0], c[1] = c[1], c[0]
	checkSortingPermutationResult(objectsFromS3, t) //213
	c[2], c[0] = c[0], c[2]
	checkSortingPermutationResult(objectsFromS3, t) //312
	c[2], c[1] = c[1], c[2]
	checkSortingPermutationResult(objectsFromS3, t) //321
	c[0], c[1] = c[1], c[0]
	checkSortingPermutationResult(objectsFromS3, t) //231
	c[2], c[0] = c[0], c[2]
	checkSortingPermutationResult(objectsFromS3, t) //132

}

func checkSortingPermutationResult(objectsFromS3 *s3.ListObjectsOutput, t *testing.T) {
	//t.Log(objectsFromS3)
	slice := walg.GetBackupTimeSlices(objectsFromS3.Contents)
	assert.Equalf(t, "backup01", slice[0].Name, "Sorting does not work correctly")
	assert.Equalf(t, "backup02", slice[1].Name, "Sorting does not work correctly")
	assert.Equalf(t, "backup03", slice[2].Name, "Sorting does not work correctly")
}
