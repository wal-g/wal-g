package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
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
func getRedisConnection(strict bool) *redis.Client {
	redisAddr := GetSettingWithLocalDefault("WALG_REDIS_HOST", "localhost")
	redisPort := GetSettingWithLocalDefault("WALG_REDIS_PORT", "6379")
	redisUsername := GetSettingWithLocalDefault(conf.RedisUsername, "default") // no user set
	redisPassword := GetSettingWithLocalDefault(conf.RedisPassword, "")        // no password set
	redisDBStr, ok := conf.GetSetting("WALG_REDIS_DB")
	redisDB := 0 // use default DB
	if ok {
		redisDBValue, err := strconv.Atoi(redisDBStr)
		// DISCUSS: could redisDB changed on success without additional variable redisDBValue?
		if strict {
			tracelog.ErrorLogger.FatalOnError(err)
		}
		redisDB = redisDBValue
	}
	return redis.NewClient(&redis.Options{
		Addr:     redisAddr + ":" + redisPort,
		Username: redisUsername,
		Password: redisPassword,
		DB:       redisDB,
	})
}

type MemoryData struct {
	UsedMemory    int64 `json:"used_memory"`
	UsedMemoryRss int64 `json:"used_memory_rss"`
}

type MemoryDataGetter struct{}

func NewMemoryDataGetter() MemoryDataGetter {
	return MemoryDataGetter{}
}

func parseInfoLine(line, name string) (i int64) {
	var err error
	if strings.HasPrefix(line, fmt.Sprintf("%s:", name)) {
		s := strings.Split(line, ":")[1]
		i, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			tracelog.InfoLogger.Printf("%s parsing from %s to int64 failed: %v", name, line, err)
			return
		}
	}
	return
}

const dontPanic = false

func (m *MemoryDataGetter) Get() (res *MemoryData) {
	res = &MemoryData{}
	conn := getRedisConnection(dontPanic)
	ctx := context.Background()
	data, err := conn.Info(ctx, "memory").Result()
	if err != nil {
		tracelog.InfoLogger.Printf("memory info getting failed: %v", err)
		return
	}
	data = strings.ReplaceAll(data, "\r", "")
	for _, line := range strings.Split(data, "\n") {
		if i := parseInfoLine(line, "used_memory"); i != 0 {
			res.UsedMemory = i
		}
		if i := parseInfoLine(line, "used_memory_rss"); i != 0 {
			res.UsedMemoryRss = i
		}
	}
	return
}
