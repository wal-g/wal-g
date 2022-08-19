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
	"github.com/wal-g/wal-g/internal"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

	BackupLastTS primitive.Timestamp `json:"BackupLastTS,omitempty"` // for binary backup
}

func (backupMeta BackupMeta) GetBackupLastTS() OpTimestamp {
	emptyTS := OpTimestamp{}
	backupLastTS := ToOpTimestamp(backupMeta.BackupLastTS)
	if backupLastTS != emptyTS {
		return backupLastTS
	}
	if backupMeta.Before.LastMajTS != emptyTS {
		return backupMeta.Before.LastMajTS
	}
	return emptyTS
}

func ToOpTimestamp(ts primitive.Timestamp) OpTimestamp {
	return OpTimestamp{TS: ts.T, Inc: ts.I}
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
	re := regexp.MustCompile(`(stream|binary)_\d{8}T\d{6}Z`)
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

func (w *WalgUtil) runCmd(run ...string) (ExecResult, error) {
	command := []string{w.cliPath, "--config", w.confPath}
	command = append(command, run...)

	exc, err := RunCommandStrict(w.ctx, w.host, command)
	return exc, err
}

func (w *WalgUtil) PushBackup() (string, error) {
	PgDataSettingString, ok := internal.GetSetting(internal.PgDataSetting)
	if !ok {
		tracelog.InfoLogger.Print("\nPGDATA is not set in the conf.\n")
	}
	if w.cliPath != PgDataSettingString {
		tracelog.WarningLogger.Printf("cliPath '%s' differ from conf PGDATA '%s'\n", w.cliPath, PgDataSettingString)
	}
	exec, err := w.runCmd("backup-push")
	if err != nil {
		return "", err
	}
	return BackupNameFromCreate(exec.Combined()), nil
}

func (w *WalgUtil) PushBinaryBackup() (string, error) {
	exec, err := w.runCmd("binary-backup-push")
	if err != nil {
		return "", err
	}
	return BackupNameFromCreate(exec.Combined()), nil
}

func (w *WalgUtil) GetBackupByNumber(backupNumber int) (string, error) {
	backups, err := w.Backups()
	if err != nil {
		return "", err
	}
	if backupNumber >= len(backups) {
		return "", fmt.Errorf("only %d backups exists, backup #%d is not found", len(backups), backupNumber)
	}
	return backups[backupNumber], nil
}

func (w *WalgUtil) FetchBackupByNum(backupNum int) error {
	backup, err := w.GetBackupByNumber(backupNum)
	if err != nil {
		return err
	}
	_, err = w.runCmd("backup-fetch", backup)
	return err
}

func (w *WalgUtil) FetchBinaryBackup(backup, mongodConfigPath, mongodbVersion string) error {
	_, err := w.runCmd("binary-backup-fetch", backup, mongodConfigPath, mongodbVersion)
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

	exec, err := w.runCmd("backup-show", backups[backupNum])
	if err != nil {
		return Sentinel{}, fmt.Errorf("backup show failed: %v", err)
	}

	var sentinel Sentinel
	err = json.Unmarshal([]byte(exec.Stdout()), &sentinel)

	return sentinel, err
}

func (w *WalgUtil) Backups() ([]string, error) {
	exec, err := w.runCmd("backup-list")
	if err != nil {
		return nil, err
	}
	return BackupNamesFromListing(exec.Combined()), nil
}

func (w *WalgUtil) PurgeRetain(keepNumber int) error {
	_, err := w.runCmd("delete",
		"--retain-count", strconv.Itoa(keepNumber),
		"--retain-after", time.Now().Format("2006-01-02T15:04:05Z"),
		"--purge-oplog",
		"--confirm")
	return err
}

func (w *WalgUtil) DeleteBackup(backupName string) error {
	_, err := w.runCmd("backup-delete", backupName, "--confirm")
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
	_, err := w.runCmd("oplog-replay", from.String(), until.String())
	return err
}

func (w *WalgUtil) OplogPurge() error {
	_, err := w.runCmd("oplog-purge", "--confirm")
	return err
}
