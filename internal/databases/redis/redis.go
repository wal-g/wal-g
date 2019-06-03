package redis

import (
	"github.com/go-redis/redis"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"strconv"
)

type Uploader struct {
	*internal.Uploader
}
type Backup struct {
	*internal.Backup
}

func getRedisConnection() *redis.Client {
	redisAddr := internal.GetSettingWithLocalDefault("WALG_REDIS_HOST", "localhost")
	redisPort := internal.GetSettingWithLocalDefault("WALG_REDIS_PORT", "6379")
	redisPassword := internal.GetSettingWithLocalDefault("WALG_REDIS_PASSWORD", "") // no password set
	redisDbStr, ok := internal.GetSetting("WALG_REDIS_DB")
	redisDb := 0 // use default DB
	if ok {
		redisDbValue, err := strconv.Atoi(redisDbStr) // DISCUSS: could redisDb changed on success without additional variable redisDbValue?
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		redisDb = redisDbValue
	}
	return redis.NewClient(&redis.Options{
		Addr:     redisAddr + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDb,
	})
}
