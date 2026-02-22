package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/logging"
)

const dontPanic = false

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
			logging.FatalOnError(err)
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

type ServerData struct {
	UsedMemory    int64 `json:"used_memory"`
	UsedMemoryRss int64 `json:"used_memory_rss"`
	MaxDBNumber   int64 `json:"max_db_number"`
}

type ServerDataGetter struct {
	conn *redis.Client
}

func NewServerDataGetter() ServerDataGetter {
	return ServerDataGetter{
		conn: getRedisConnection(dontPanic),
	}
}

func parseInfoLine(line, name string) (i int64) {
	// used_memory:20019376
	var err error
	if strings.HasPrefix(line, fmt.Sprintf("%s:", name)) {
		s := strings.Split(line, ":")[1]
		i, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			slog.Info(fmt.Sprintf("%s parsing from %s to int64 failed: %v", name, line, err))
			return
		}
	}
	return
}

func (m *ServerDataGetter) fillMemoryData(res *ServerData) {
	ctx := context.Background()
	data, err := m.conn.Info(ctx, "memory").Result()
	if err != nil {
		slog.Info(fmt.Sprintf("memory info getting failed: %v", err))
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
}

func parseInfoLineNumberedName(line, name string) (i int64) {
	// db0:keys=2,expires=0,avg_ttl=0
	var err error
	if strings.HasPrefix(line, name) {
		numberedName := strings.Split(line, ":")[0]
		number := strings.Split(numberedName, name)[1]
		i, err = strconv.ParseInt(number, 10, 64)
		if err != nil {
			slog.Info(fmt.Sprintf("%s parsing from %s to int64 failed: %v", name, line, err))
			return
		}
	}
	return
}

func (m *ServerDataGetter) fillMaxDBNumData(res *ServerData) {
	ctx := context.Background()
	data, err := m.conn.Info(ctx, "keyspace").Result()
	if err != nil {
		slog.Info(fmt.Sprintf("keyspace info getting failed: %v", err))
		return
	}
	data = strings.ReplaceAll(data, "\r", "")
	for _, line := range strings.Split(data, "\n") {
		i := parseInfoLineNumberedName(line, "db")
		if i > res.MaxDBNumber {
			res.MaxDBNumber = i
		}
	}
}

func (m *ServerDataGetter) Get() (res *ServerData) {
	res = &ServerData{}
	m.fillMemoryData(res)
	m.fillMaxDBNumData(res)
	return
}
