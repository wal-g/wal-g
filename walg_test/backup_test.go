package walg_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
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

	bk := &walg.Backup{
		Folder: folder,
		Path:   walg.GetBackupPath(folder),
		Name:   aws.String("base_backupmockBackup"),
	}

	//CheckExistence error testing
	exists, _ := bk.CheckExistence()
	if exists {
		t.Errorf("backup: expected mock backup to not exist")
	}

	folder.S3API = testtools.NewMockS3Client(true, false)
	_, err := bk.CheckExistence()
	if err == nil {
		t.Errorf("backup: CheckExistence expected error but got '<nil>'")
	}

	//GetLatest error testing
	_, err = bk.GetLatest()
	if err == nil {
		t.Errorf("backup: expected error but got '<nil>'")
	}

	//GetKeys error testing
	_, err = bk.GetKeys()
	if err == nil {
		t.Errorf("backup: expected error but got '<nil>'")
	}

	//Test S3 ReaderMaker with error S3.
	keys := []string{"1.nop", "2.nop", "3.gzip", "4.lzo"}
	n := &testtools.NOPTarInterpreter{}

	out := make([]walg.ReaderMaker, len(keys))
	for i, key := range keys {
		s := &walg.S3ReaderMaker{
			Backup:     bk,
			Key:        aws.String(key),
			FileFormat: walg.GetFileExtension(key),
		}
		out[i] = s
	}

	err = walg.ExtractAll(n, out)
	if err == nil {
		t.Errorf("backup: expected error but got '<nil>'")
	}
}

// Tests backup-fetch methods including:
// GetLatest()
// CheckExistence()
// GetKeys()
func TestBackup(t *testing.T) {
	folder := testtools.NewMockS3Folder(false, false)

	bk := &walg.Backup{
		Folder: folder,
		Path:   walg.GetBackupPath(folder),
		Name:   aws.String("base_backupmockBackup"),
	}

	latest, _ := bk.GetLatest()
	if latest != "first.nop" {
		t.Errorf("backup: expected %s from 'GetLatest' but got %s", "first", latest)
	}

	exists, _ := bk.CheckExistence()
	if !exists {
		t.Errorf("backup: expected mock backup to exist but 'CheckExistence' returned false")
	}

	keys, err := bk.GetKeys()
	if err != nil {
		t.Errorf("backup: expected no error but got %+v\n", err)
	}

	for i, val := range correctKeys {
		if keys[i] != val {
			t.Errorf("backup: expected %s but got %s", val, keys[i])
		}
	}

	// Test S3 ReaderMaker
	n := &testtools.NOPTarInterpreter{}

	out := make([]walg.ReaderMaker, len(keys))
	for i, key := range keys {
		s := &walg.S3ReaderMaker{
			Backup:     bk,
			Key:        aws.String(key),
			FileFormat: walg.GetFileExtension(key),
		}
		out[i] = s
		if out[i].Path() != correctKeys[i] {
			t.Errorf("backup: expected S3ReaderMaker key to be %s but got %s", correctKeys[i], out[i].Path())
		}
	}

	err = walg.ExtractAll(n, out)
	if err != nil {
		t.Errorf("backup: could not extract from S3ReaderMaker")
	}

}

func TestArchiveErrors(t *testing.T) {
	folder := testtools.NewMockS3Folder(true, true)

	arch := &walg.Archive{
		Folder:  folder,
		Archive: aws.String("mockArchive"),
	}

	// CheckExistence error testing
	exists, _ := arch.CheckExistence()
	if exists {
		t.Errorf("archive: expected mock archive to not exist")
	}

	folder.S3API = testtools.NewMockS3Client(true, false)
	_, err := arch.CheckExistence()
	if err == nil {
		t.Errorf("archive: CheckExistence expected error but got `<nil>`")
	}

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
	if !exists {
		t.Errorf("archive: expected mock archive to exist but 'CheckExistence' returned false")
	}

	body, err := arch.GetArchive()
	if err != nil {
		t.Errorf("archive: expected no error but got %+v\n", err)
	}

	allBody, err := ioutil.ReadAll(body)

	if err != nil {
		t.Log(err)
	}

	if string(allBody[:]) != "mock content" {
		t.Errorf("archive: expected archive body to be %s but got %v", "mock content", allBody)
	}

	folder.S3API = testtools.NewMockS3Client(true, false)

	_, err = arch.CheckExistence()
	if err == nil {
		t.Errorf("archive: CheckExistence expected error but got `<nil>`")
	}

	_, err = arch.GetArchive()
	if err == nil {
		t.Errorf("archive: expected error but got %v", err)
	}
}

func TestGetBackupTimeSlices(t *testing.T) {
	first := "mockServer/backup01_backup_stop_sentinel.json"
	second := "mockServer/somedir/backup02_backup_stop_sentinel.json"
	third := "mockServer/somedir/somesubdir/backup03_backup_stop_sentinel.json"
	firstTime := time.Now().Add(time.Hour)
	secondTime := time.Now().Add(time.Minute)
	thirdTime := time.Now()

	c := []*s3.Object{
		{Key: &first, LastModified: &firstTime},
		{Key: &second, LastModified: &secondTime},
		{Key: &third, LastModified: &thirdTime},
	}
	objectsFromS3 := &s3.ListObjectsV2Output{Contents: c}

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

func checkSortingPermutationResult(objectsFromS3 *s3.ListObjectsV2Output, t *testing.T) {
	//t.Log(objectsFromS3)
	slice := walg.GetBackupTimeSlices(objectsFromS3.Contents)
	if slice[0].Name != "backup01" {
		t.Log(slice[0].Name)
		t.Error("Sorting does not work correctly")
	}
	if slice[1].Name != "backup02" {
		t.Log(slice[1].Name)
		t.Error("Sorting does not work correctly")
	}
	if slice[2].Name != "backup03" {
		t.Log(slice[2].Name)
		t.Error("Sorting does not work correctly")
	}
}
