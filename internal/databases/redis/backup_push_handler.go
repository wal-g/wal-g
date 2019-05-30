package redis

import (
	"bufio"
	"github.com/go-redis/redis"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"math"
	"os"
	"time"
)

func HandleBackupPush(uploader *Uploader) {
	// Configure folders
	redisDataFoler, ok := internal.GetSetting("WALG_REDIS_DATA_FOLDER")
	if !ok {
		redisDataFoler = "/var/lib/redis"
	}
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	// Get Redis connection
	client := getRedisConnection()

	// Init backup process
	currentTime := time.Now()
	initializeBackupSaving(client)

	// Wait for new backup on disk.
	// DISCUSS: Maybe add Timer to stop trying after some time?
	waitForNewBackup(currentTime, redisDataFoler)

	// Upload backup
	uploadBackup(uploader, redisDataFoler)
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

		if math.Abs(currentTime.Sub(stat.ModTime()).Seconds()) < 10 {
			tracelog.InfoLogger.Println("Backup has been made not so long ago, not going to wait for BGSAVE")
			break
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
	namedReader := internal.NewNamedReaderImpl(reader, "base_"+time.Now().Format(time.RFC3339)) // DISCUSS: Is it correct backup name?
	err = uploader.UploadFile(namedReader)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}
