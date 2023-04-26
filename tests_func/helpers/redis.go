package helpers

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/wal-g/tracelog"
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

func (rc *RedisCtl) Host() string {
	return rc.host
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

func (rc *RedisCtl) runCmd(run []string) (ExecResult, error) {
	command := []string{rc.binPath, "--config", rc.confPath}
	command = append(command, run...)

	exc, err := RunCommandStrict(rc.ctx, rc.host, command)
	return exc, err
}

func (rc *RedisCtl) PurgeRetain(keepNumber int) error {
	_, err := rc.runCmd([]string{
		"delete",
		"--retain-count", strconv.Itoa(keepNumber),
		"--retain-after", time.Now().Format("2006-01-02T15:04:05Z"),
		"--confirm"})
	return err
}
