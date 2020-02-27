package redis

import (
	"strconv"

	"github.com/go-redis/redis"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

// DISCUSS: In some cases, we have default values, but we don't want to store it at global default settings.
// Naming is far from best, if Go allowed overloads, name GetSettingWithDefault would be more appropriate
func GetSettingWithLocalDefault(key string, defaultValue string) string {
	value, ok := internal.GetSetting(key)
	if ok {
		return value
	}
	return defaultValue
}

func getRedisConnection() *redis.Client {
	redisAddr := GetSettingWithLocalDefault("WALG_REDIS_HOST", "localhost")
	redisPort := GetSettingWithLocalDefault("WALG_REDIS_PORT", "6379")
	redisPassword := GetSettingWithLocalDefault("WALG_REDIS_PASSWORD", "") // no password set
	redisDbStr, ok := internal.GetSetting("WALG_REDIS_DB")
	redisDb := 0 // use default DB
	if ok {
		redisDbValue, err := strconv.Atoi(redisDbStr) // DISCUSS: could redisDb changed on success without additional variable redisDbValue?
		tracelog.ErrorLogger.FatalOnError(err)
		redisDb = redisDbValue
	}
	return redis.NewClient(&redis.Options{
		Addr:     redisAddr + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDb,
	})
}
