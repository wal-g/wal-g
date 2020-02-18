package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
)

type Sentinel struct {
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
	MongoMeta       BackupMeta  `json:"MongoMeta,omitempty"`
}

type NodeMeta struct {
	LastTS    OpTimestamp `json:"LastTS,omitempty"`
	LastMajTS OpTimestamp `json:"LastMajTS,omitempty"`
}

type BackupMeta struct {
	Before NodeMeta `json:"Before,omitempty"`
	After  NodeMeta `json:"After,omitempty"`
}

type OpTimestamp struct {
	TS  uint32
	Inc uint32
}

type Archive struct {
	StartTS OpTimestamp
	EndTS   OpTimestamp
}

func (ots OpTimestamp) String() string {
	return fmt.Sprintf("%d.%d", ots.TS, ots.Inc)
}

func LessTS(ots1, ots2 OpTimestamp) bool {
	if ots1.TS < ots2.TS {
		return true
	}
	if ots1.TS > ots2.TS {
		return false
	}
	return ots1.Inc < ots2.Inc
}

func TimestampFromStr(s string) (OpTimestamp, error) {
	strs := strings.Split(s, ".")
	if len(strs) != 2 {
		return OpTimestamp{}, fmt.Errorf("can not split oplog ts string '%s': two parts expected", s)
	}

	ts, err := strconv.ParseUint(strs[0], 10, 32)
	if err != nil {
		return OpTimestamp{}, fmt.Errorf("can not convert ts string '%v': %w", ts, err)
	}
	inc, err := strconv.ParseUint(strs[1], 10, 32)
	if err != nil {
		return OpTimestamp{}, fmt.Errorf("can not convert inc string '%v': %w", inc, err)
	}

	return OpTimestamp{TS: uint32(ts), Inc: uint32(inc)}, nil
}

func BackupNamesFromListing(output string) []string {
	re := regexp.MustCompile("stream_[0-9]{8}T[0-9]{6}Z")
	return re.FindAllString(output, -1)
}

func BackupNameFromCreate(output string) string {
	return strings.Trim(strings.Split(output, "FILE PATH: ")[1], " ")
}

type WalgUtil struct {
	ctx      context.Context
	host     string
	cliPath  string
	confPath string
	mongoMaj string
}

func NewWalgUtil(ctx context.Context, host, cliPath, confPath, mongoMaj string) *WalgUtil {
	return &WalgUtil{ctx, host, cliPath, confPath, mongoMaj}
}

func (w *WalgUtil) runCmd(run []string) (ExecResult, error) {
	command := []string{w.cliPath, "--config", w.confPath}
	command = append(command, run...)

	exc, err := RunCommandStrict(w.ctx, w.host, command)
	return exc, err
}

func (w *WalgUtil) PushBackup() (string, error) {
	exec, err := w.runCmd([]string{"backup-push"})
	if err != nil {
		return "", err
	}
	return BackupNameFromCreate(exec.Combined()), nil
}

func (w *WalgUtil) FetchBackupByNum(backupNum int) error {
	backups, err := w.Backups()
	if err != nil {
		return err
	}
	if backupNum >= len(backups) {
		return fmt.Errorf("only %d backups exists, backup #%d is not found", len(backups), backupNum)
	}

	walgCommand := []string{w.cliPath, "--config", w.confPath, "backup-fetch", backups[backupNum]}
	mongoCommand := []string{"|", "mongorestore", "--archive", "--preserveUUID", "--drop", "--uri=\"mongodb://admin:password@127.0.0.1:27018\""}
	if w.mongoMaj == "3.6" {  // TODO: refactor
		mongoCommand = []string{"|", "mongorestore", "--archive", "--drop", "--uri=\"mongodb://admin:password@127.0.0.1:27018\""}
	}
	command := strings.Join(append(walgCommand, mongoCommand...), " ")
	_, err = RunCommandStrict(w.ctx, w.host, []string{"bash", "-c", command})

	return err
}

func (w *WalgUtil) BackupMeta(backupNum int) (Sentinel, error) {
	backups, err := w.Backups()
	if err != nil {
		return Sentinel{}, err
	}
	if backupNum >= len(backups) {
		return Sentinel{}, fmt.Errorf("only %d backups exists, backup #%d is not found", len(backups), backupNum)
	}

	exec, err := w.runCmd([]string{"backup-show", backups[backupNum]})
	if err != nil {
		return Sentinel{}, fmt.Errorf("backup show failed: %v", err)
	}

	var sentinel Sentinel
	err = json.Unmarshal([]byte(exec.Stdout()), &sentinel)

	return sentinel, err
}

func (w *WalgUtil) Backups() ([]string, error) {
	exec, err := w.runCmd([]string{"backup-list"})
	if err != nil {
		return nil, err
	}
	return BackupNamesFromListing(exec.Combined()), nil
}

func (w *WalgUtil) PurgeAll() error {
	_, err := w.runCmd([]string{"delete", "everything", "--confirm"})
	return err
}

func (w *WalgUtil) PurgeRetain(keepNumber int) error {
	_, err := w.runCmd([]string{"delete", "retain", strconv.Itoa(keepNumber), "--confirm"})
	return err
}

func (w *WalgUtil) PurgeAfterNum(keepNumber int, afterBackupNum int) error {
	backups, err := w.Backups()
	if err != nil {
		return err
	}

	if afterBackupNum >= len(backups) {
		return fmt.Errorf("only %d backups exists, backup #%d is not found", len(backups), afterBackupNum)
	}

	_, err = w.runCmd([]string{
		"delete", "retain", strconv.Itoa(keepNumber), "--after", backups[len(backups)-afterBackupNum-1], "--confirm"})

	return err
}

func (w *WalgUtil) PurgeAfterTime(keepNumber int, timeLine time.Time) error {
	_, err := w.runCmd([]string{
		"delete", "retain", strconv.Itoa(keepNumber), "--after", timeLine.Format(time.RFC3339), "--confirm",
	})
	return err
}

func (w *WalgUtil) OplogPush() error {
	cmd := []string{w.cliPath, "--config", w.confPath, "oplog-push"}
	cmdLine := strings.Join(cmd, " ")

	exc, err := RunCommand(w.ctx, w.host, cmd)

	if err != nil || exc.ExitCode != 0 {
		tracelog.DebugLogger.Printf("'%s' failed with error %s, exit code %d, stdout:\n%s\nstderr:\n%s\n",
			cmdLine, err, exc.ExitCode, exc.Stdout(), exc.Stderr())
		return fmt.Errorf("%s exit code: %d", cmdLine, exc.ExitCode)
	}

	return nil
}

func (w *WalgUtil) OplogReplay(from, until OpTimestamp) error {
	_, err := w.runCmd([]string{"oplog-replay", from.String(), until.String()})
	return err
}
