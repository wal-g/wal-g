package functests

import (
	"fmt"
	"github.com/DATA-DOG/godog"
	"github.com/go-redis/redis"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"strconv"
)

func (tctx *TestContext) isWorkingRedis(hostName string) error {
	host := tctx.ContainerFQDN(hostName)
	port, err := strconv.Atoi(tctx.Env["REDIS_EXPOSE_PORT"])
	expHost, expPort, err := helpers.ExposedHostPort(tctx.Context, host, port)
	if err != nil {
		return fmt.Errorf("Expose host failed: %v\n", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:     expHost + ":" + strconv.Itoa(expPort),
		Password: "password",
		DB:       0,
	})

	return helpers.Retry(tctx.Context, 10, func() error {
		tracelog.DebugLogger.Printf("Redis client connect to host '%s:%d'", expHost, expPort)

		status := client.Ping()
		err = status.Err()
		if err != nil {
			return fmt.Errorf("Client on ping returned err: %v\n", err)
		}
		if status.Val() != "PONG" {
			return fmt.Errorf("Client on ping does not returned PONG: %v\n", err)
		}
		return nil
	})
}

func (tctx *TestContext) redisHasTestRedisDataTest(arg1 string, arg2 int) error {
	return godog.ErrPending
}


func weCreateRedisBackup(arg1 int) error {
	return godog.ErrPending
}