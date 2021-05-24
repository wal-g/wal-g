package helpers

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/wal-g/tracelog"
	"math/rand"
	"strconv"
)

type RedisCtl struct {
	host     string
	port     int
	binPath  string
	confPath string
	*redis.Client
	ctx context.Context
}

func NewRedisCtl(ctx context.Context, host string, port int, password, binPath, confPath string) (*RedisCtl, error) {
	expHost, expPort, err := ExposedHostPort(ctx, host, port)
	if err != nil {
		return nil, fmt.Errorf("Expose host failed: %v\n", err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:     expHost + ":" + strconv.Itoa(expPort),
		Password: password,
		DB:       0,
	})
	return &RedisCtl{
		host,
		port,
		binPath,
		confPath,
		client,
		ctx,
	}, nil
}

func (rc *RedisCtl) Addr() string {
	return rc.Options().Addr
}

type Strings struct {
	arraylist []string
}

func (rc *RedisCtl) WriteTestData(mark string, docsCount int) error {
	var rows []interface{}
	for k := 1; k <= docsCount; k++ {
		var data interface{}
		switch rand.Intn(3) {
		case 1:
			data = "string_val"
		case 2:
			data = 100500
		case 3:
			data = Strings{[]string{"hello", "there"}}
		}
		rows = append(rows, fmt.Sprintf("%s_key_num%d", mark, k), data)
	}
	status := rc.MSet(rows...)
	tracelog.DebugLogger.Printf("WriteTestData result: %v", status)
	if status.Err() != nil {
		return fmt.Errorf("failed to write test data to redis: %w", status.Err())
	}
	return nil
}

func (rc *RedisCtl) PushBackup() (string, error) {
	exec, err := rc.runCmd([]string{"backup-push"})
	if err != nil {
		return "", err
	}
	return BackupNameFromCreate(exec.Combined()), nil
}

func (w *RedisCtl) runCmd(run []string) (ExecResult, error) {
	command := []string{w.binPath, "--config", w.confPath}
	command = append(command, run...)

	exc, err := RunCommandStrict(w.ctx, w.host, command)
	return exc, err
}
