package walg_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/katie31/wal-g"
	"github.com/katie31/wal-g/testTools"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

var correctKeys = []string{"mockServer/base_backup/second.nop",
	"mockServer/base_backup/fourth.nop",
	"mockServer/base_backup/fifth.nop",
	"mockServer/base_backup/first.nop",
	"mockServer/base_backup/third.nop"}

/**
 *  Mock out S3 client. Includes these methods:
 *  ListObjectsV2(*ListObjectsV2Input)
 *  GetObject(*GetObjectInput)
 *  HeadObject(*HeadObjectInput)
 */
type mockS3Client struct {
	s3iface.S3API
	notFound bool
	err      bool
}

func (m *mockS3Client) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	if m.err {
		return nil, awserr.New("MockListObjects", "mock ListObjectsV2 errors", nil)
	}

	contents := fakeContents()
	output := &s3.ListObjectsV2Output{
		Contents: contents,
		Name:     input.Bucket,
	}

	return output, nil
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if m.err {
		return nil, awserr.New("MockGetObject", "mock GetObject error", nil)
	}

	output := &s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader("mock content")),
	}

	return output, nil
}

func (m *mockS3Client) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if m.err && m.notFound {
		return nil, awserr.New("NotFound", "mock HeadObject error", nil)
	} else if m.err {
		return nil, awserr.New("MockHeadObject", "mock HeadObject error", nil)
	}

	return &s3.HeadObjectOutput{}, nil
}

/**
 *  Mock out uploader client for S3. Includes these methods:
 *  Upload(*UploadInput, ...func(*s3manager.Uploader))
 */
type mockS3Uploader struct {
	s3manageriface.UploaderAPI
	err bool
}

func (u *mockS3Uploader) Upload(input *s3manager.UploadInput, f ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if u.err {
		return nil, awserr.New("UploadFailed", "mock Upload error", nil)
	}

	output := &s3manager.UploadOutput{
		Location:  *input.Bucket,
		VersionID: input.Key,
	}

	/***	Discard bytes to unblock pipe.	***/
	_, err := io.Copy(ioutil.Discard, input.Body)
	if err != nil {
		return nil, err
	}

	return output, nil
}

/**
 *  Creates 5 fake s3 objects with Key and LastModified field.
 */
func fakeContents() []*s3.Object {
	c := make([]*s3.Object, 5)

	ob := &s3.Object{
		Key:          aws.String("mockServer/base_backup/second.nop"),
		LastModified: aws.Time(time.Date(2017, 2, 2, 30, 48, 39, 651387233, time.UTC)),
	}
	c[0] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/fourth.nop"),
		LastModified: aws.Time(time.Date(2009, 2, 27, 20, 8, 33, 651387235, time.UTC)),
	}
	c[1] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/fifth.nop"),
		LastModified: aws.Time(time.Date(2008, 11, 20, 16, 34, 58, 651387232, time.UTC)),
	}
	c[2] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/first.nop"),
		LastModified: aws.Time(time.Date(2020, 11, 31, 20, 3, 58, 651387237, time.UTC)),
	}
	c[3] = ob

	ob = &s3.Object{
		Key:          aws.String("mockServer/base_backup/third.nop"),
		LastModified: aws.Time(time.Date(2009, 3, 13, 4, 2, 42, 651387234, time.UTC)),
	}
	c[4] = ob

	return c
}

/**
 *  Test backup error handling.
 */
func TestBackupErrors(t *testing.T) {
	pre := &walg.Prefix{
		Svc: &mockS3Client{
			err:      true,
			notFound: true,
		},
		Bucket: aws.String("mock bucket"),
		Server: aws.String("mock server"),
	}

	bk := &walg.Backup{
		Prefix: pre,
		Path:   aws.String(*pre.Server + "/basebackups_005/"),
		Name:   aws.String("base_backupmockBackup"),
	}

	/***	CheckExistence error testing	***/
	exists := bk.CheckExistence()
	if exists {
		t.Errorf("backup: expected mock backup to not exist")
	}

	pre.Svc = &mockS3Client{
		err: true,
	}
	exists = bk.CheckExistence()
	if exists {
		t.Errorf("backup: expected mock backup to fail")
	}

	/***	GetLatest error testing 	***/
	_, err := bk.GetLatest()
	if err == nil {
		t.Errorf("backup: expected error but got '<nil>'")
	}

	/***	GetKeys error testing 	***/
	_, err = bk.GetKeys()
	if err == nil {
		t.Errorf("backup: expected error but got '<nil>'")
	}

	/***	Test S3 ReaderMaker with error S3.	***/
	keys := []string{"1.nop", "2.nop", "3.gzip"}
	n := &tools.NOPTarInterpreter{}

	out := make([]walg.ReaderMaker, len(keys))
	for i, key := range keys {
		s := &walg.S3ReaderMaker{
			Backup:     bk,
			Key:        aws.String(key),
			FileFormat: walg.CheckType(key),
		}
		out[i] = s
	}

	err = walg.ExtractAll(n, out)
	if err == nil {
		t.Errorf("backup: expected error but got '<nil>'")
	} else {
		t.Logf("%+v\n", err)
	}
}

/**
 *  Tests that backup fetch methods work. Tests:
 *  GetLatest()
 *  CheckExistence()
 *  GetKeys()
 */
func TestBackup(t *testing.T) {
	pre := &walg.Prefix{
		Svc:    &mockS3Client{},
		Bucket: aws.String("mock bucket"),
		Server: aws.String("mock server"),
	}

	bk := &walg.Backup{
		Prefix: pre,
		Path:   aws.String(*pre.Server + "/basebackups_005/"),
		Name:   aws.String("base_backupmockBackup"),
	}

	latest, _ := bk.GetLatest()
	if latest != "first.nop" {
		t.Errorf("backup: expected %s from 'GetLatest' but got %s", "first", latest)
	}

	exists := bk.CheckExistence()
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

	/***	Test S3 ReaderMaker		***/
	n := &tools.NOPTarInterpreter{}

	out := make([]walg.ReaderMaker, len(keys))
	for i, key := range keys {
		s := &walg.S3ReaderMaker{
			Backup:     bk,
			Key:        aws.String(key),
			FileFormat: walg.CheckType(key),
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
	pre := &walg.Prefix{
		Svc: &mockS3Client{
			err:      true,
			notFound: true,
		},
		Bucket: aws.String("mock bucket"),
		Server: aws.String("mock server"),
	}

	arch := &walg.Archive{
		Prefix:  pre,
		Archive: aws.String("mockArchive"),
	}

	/***	CheckExistence error testing	***/
	exists := arch.CheckExistence()
	if exists {
		t.Errorf("archive: expected mock archive to not exist")
	}

	pre.Svc = &mockS3Client{
		err: true,
	}
	exists = arch.CheckExistence()
	if exists {
		t.Errorf("archive: expected mock archive to fail")
	}

}

/**
 *  Tests Archive functions including:
 *  CheckExistence()
 *  GetArchive()
 */
func TestArchive(t *testing.T) {
	pre := &walg.Prefix{
		Svc:    &mockS3Client{},
		Bucket: aws.String("mock bucket"),
		Server: aws.String("mock server"),
	}

	arch := &walg.Archive{
		Prefix:  pre,
		Archive: aws.String("mockArchive"),
	}

	exists := arch.CheckExistence()
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

	pre.Svc = &mockS3Client{
		err: true,
	}

	exists = arch.CheckExistence()
	if exists {
		t.Errorf("archive: expected mock backup to not exist")
	}
}
