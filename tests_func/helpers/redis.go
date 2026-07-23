package helpers

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wal-g/tracelog"
)

type RedisCtl struct {
	*redis.Client
	ctx      context.Context
	binPath  string
	confPath string
	host     string
	port     int
}

type RedisCtlArgs struct {
	BinPath  string
	ConfPath string

	Host string
	Port int

	Username string
	Password string
}

func NewRedisCtl(ctx context.Context, args RedisCtlArgs) (*RedisCtl, error) {
	expHost, expPort, err := ExposedHostPort(ctx, args.Host, args.Port)
	if err != nil {
		return nil, fmt.Errorf("expose host failed: %v", err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:     expHost + ":" + strconv.Itoa(expPort),
		DB:       0,
		Password: args.Password,
		Username: args.Username,
	})
	return &RedisCtl{
		client,
		ctx,
		args.BinPath,
		args.ConfPath,
		args.Host,
		args.Port,
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
	status := rc.MSet(rc.ctx, rows...)
	tracelog.DebugLogger.Printf("WriteTestData result: %v", status)
	if status.Err() != nil {
		return fmt.Errorf("failed to write test data to redis: %w", status.Err())
	}
	return nil
}

func (rc *RedisCtl) PushBackup(backupType string) (string, error) {
	exec, err := rc.runCmd([]string{"backup-push", "--type", backupType})
	if err != nil {
		return "", err
	}
	if backupType == "rdb" {
		return BackupNameFromCreate(exec.Combined()), nil
	}
	return "", nil
}

func (rc *RedisCtl) PushTSBackup(backupType, sourceDir, backupID string) error {
	args := []string{"backup-push", "--type", backupType, "--ts-backup", sourceDir}
	if backupID != "" {
		args = append(args, "--ts-backup-id", backupID)
	}
	_, err := rc.runCmd(args)
	return err
}

func (rc *RedisCtl) FetchTSBackup(backupName, backupType, targetDir, redisVersion string) error {
	args := []string{"backup-fetch", backupName, "--type", backupType, "--ts-backup", targetDir}
	if redisVersion != "" {
		args = append(args, "--redis-version", redisVersion)
	}
	_, err := rc.runCmd(args)
	return err
}

func (rc *RedisCtl) Backups() ([]string, error) {
	exec, err := rc.runCmd([]string{"backup-list"})
	if err != nil {
		return nil, err
	}
	return BackupNamesFromListing(exec.Combined()), nil
}

func (rc *RedisCtl) runCmd(run []string) (ExecResult, error) {
	command := []string{rc.binPath, "--config", rc.confPath}
	command = append(command, run...)

	exc, err := RunCommandStrict(rc.ctx, rc.host, command)
	return exc, err
}

func (rc *RedisCtl) DeleteBackup(backupName string) error {
	_, err := rc.runCmd([]string{"backup-delete", backupName, "--confirm"})
	return err
}

func (rc *RedisCtl) PurgeRetain(keepNumber int) error {
	_, err := rc.runCmd([]string{
		"delete",
		"--retain-count", strconv.Itoa(keepNumber),
		"--retain-after", time.Now().Format("2006-01-02T15:04:05Z"),
		"--confirm"})
	return err
}
