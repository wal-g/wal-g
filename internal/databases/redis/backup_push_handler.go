package redis

import (
	"bufio"
	"github.com/go-redis/redis"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"os"
	"time"
)

func HandleBackupPush(uploader *Uploader) {
	// Configure folders
	redisDataFoler := internal.GetSettingValue("WALG_REDIS_DATA_FOLDER")
	if redisDataFoler == "" {
		redisDataFoler = "/var/lib/redis"
	}
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	// Get Redis connection
	client := getRedisConnection()

	// Init backup process
	currentTime := time.Now()
	initializeBackupSaving(client)

	// Wait for new backup on disk. Maybe add Timer?
	waitForNewBackup(currentTime, redisDataFoler)

	// Upload backup
	uploadBackup(uploader, redisDataFoler)
}

func getRedisConnection() *redis.Client {
	return redis.NewClient(&redis.Options{ // TODO: read from env
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func initializeBackupSaving(client *redis.Client) {
	err := client.BgSave().Err()
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

func waitForNewBackup(currentTime time.Time, redisDataFoler string) {
	for {
		if _, err := os.Stat(redisDataFoler + "/dump.rdb"); !os.IsNotExist(err) {
			break
		}
	}

	for {
		stat, err := os.Stat(redisDataFoler + "/dump.rdb")
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
		if stat.ModTime().After(currentTime) {
			break
		}
	}
}

func uploadBackup(uploader *Uploader, redisDataFoler string) {
	f, err := os.Open(redisDataFoler + "/dump.rdb")
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	defer utility.LoggedClose(f, "")
	reader := bufio.NewReader(f)
	namedReader := internal.NewNamedReaderImpl(reader, "backup") // Add date
	err = uploader.UploadFile(namedReader)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}
