package walg

import (
	"testing"
)

func TestNextWALFileName(t *testing.T) {
	nextName, err := GetNextWALFileName("000000010000000000000051")
	if err != nil || nextName != "000000010000000000000052" {
		t.Fatal("TestNextWALFileName 000000010000000000000051 failed")
	}

	nextName, err = GetNextWALFileName("00000001000000000000005F")
	if err != nil || nextName != "000000010000000000000060" {
		t.Fatal("TestNextWALFileName 00000001000000000000005F failed")
	}

	nextName, err = GetNextWALFileName("0000000100000001000000FF")
	if err != nil || nextName != "000000010000000200000000" {
		t.Fatal("TestNextWALFileName 0000000100000001000000FF failed")
	}

	_, err = GetNextWALFileName("0000000100000001000001FF")
	if err == nil {
		t.Fatal("TestNextWALFileName 0000000100000001000001FF did not failed")
	}

	_, err = GetNextWALFileName("00000001000ZZ001000000FF")
	if err == nil {
		t.Fatal("TestNextWALFileName 00000001000ZZ001000001FF did not failed")
	}

	_, err = GetNextWALFileName("00000001000001000000FF")
	if err == nil {
		t.Fatal("TestNextWALFileName 00000001000001000001FF did not failed")
	}

	_, err = GetNextWALFileName("asdfasdf")
	if err == nil {
		t.Fatal("TestNextWALFileName asdfasdf did not failed")
	}
}

func TestPrefetchLocation(t *testing.T) {
	prefetchLocation, runningLocation, runningFile, fetchedFile := getPrefetchLocations("/var/pgdata/xlog/", "000000010000000000000051")
	if prefetchLocation != "/var/pgdata/xlog/.wal-g/prefetch" ||
		runningLocation != "/var/pgdata/xlog/.wal-g/prefetch/running" ||
		runningFile != "/var/pgdata/xlog/.wal-g/prefetch/running/000000010000000000000051" ||
		fetchedFile != "/var/pgdata/xlog/.wal-g/prefetch/000000010000000000000051" {
		t.Fatal("TestPrefetchLocation failed")
	}
}
