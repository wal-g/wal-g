package redis

import (
	"strconv"

	"github.com/go-redis/redis"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
)

// DISCUSS: In some cases, we have default values, but we don't want to store it at global default settings.
// Naming is far from best, if Go allowed overloads, name GetSettingWithDefault would be more appropriate
func GetSettingWithLocalDefault(key string, defaultValue string) string {
	value, ok := conf.GetSetting(key)
	if ok {
		return value
	}
	return defaultValue
}

// getRedisConnection
func _() *redis.Client {
	redisAddr := GetSettingWithLocalDefault("WALG_REDIS_HOST", "localhost")
	redisPort := GetSettingWithLocalDefault("WALG_REDIS_PORT", "6379")
	redisPassword := GetSettingWithLocalDefault(conf.RedisPassword, "") // no password set
	redisDBStr, ok := conf.GetSetting("WALG_REDIS_DB")
	redisDB := 0 // use default DB
	if ok {
		redisDBValue, err := strconv.Atoi(redisDBStr)
		// DISCUSS: could redisDB changed on success without additional variable redisDBValue?
		tracelog.ErrorLogger.FatalOnError(err)
		redisDB = redisDBValue
	}
	return redis.NewClient(&redis.Options{
		Addr:     redisAddr + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
}
