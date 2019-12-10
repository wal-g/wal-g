package helpers

import (
	"bufio"
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func connectHostPort(context context.Context, host string, port uint16) (*mongo.Client, error) {
	uri := fmt.Sprintf("mongodb://%s:%d/?connect=direct", host, port)
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongo via host and port: %v", err)
	}
	err = client.Connect(context)
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongo via host and port: %v", err)
	}
	return client, nil
}

func connect(context context.Context, user string, password string, dbname string, host string, port uint16) (*mongo.Client, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d/%s?connect=direct&authMechanism=SCRAM-SHA-1", user, password, host, port, dbname)
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongo via host, port, dbname and user creds: %v", err)
	}
	err = client.Connect(context)
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongo via host, port, dbname and user creds: %v", err)
	}
	return client, nil
}

func EnvDBConnect(testContext *TestContextType, nodeName string) (*mongo.Client, error) {
	dbMongoPort, err := strconv.Atoi(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_EXPOSE_MONGOD"))
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongodb: %v", err)
	}
	dbHost, err := GetDockerContainer(testContext, nodeName)
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongodb: %v", err)
	}
	host, port, err := GetExposedPort(*dbHost, uint16(dbMongoPort))
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongodb: %v", err)
	}
	conn, err := connectHostPort(
		testContext.Context,
		host,
		port)
	if err != nil {
		return nil, fmt.Errorf("error in connection to mongodb: %v", err)
	}
	return conn, nil
}

func EnvDBConnectWithCreds(testContext *TestContextType, nodeName string, creds UserConfiguration) (*mongo.Client, error) {
	dbMongoPort, err := strconv.Atoi(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_EXPOSE_MONGOD"))
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongodb: %v", err)
	}
	dbHost, err := GetDockerContainer(testContext, nodeName)
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongodb: %v", err)
	}
	host, port, err := GetExposedPort(*dbHost, uint16(dbMongoPort))
	if err != nil {
		return nil, fmt.Errorf("error in connecting to mongodb: %v", err)
	}
	conn, err := connect(
		testContext.Context,
		creds.Username,
		creds.Password,
		creds.Dbname,
		host,
		port)
	if err != nil {
		return nil, fmt.Errorf("error in connection to mongodb: %v", err)
	}
	return conn, nil
}

func FillWithData(context context.Context, database *mongo.Client, mark string) map[string]map[string][]DatabaseRecord {
	var data = make(map[string]map[string][]DatabaseRecord, 0)
	for _, dbName := range []string{"test_db_01", "test_db_02"} {
		if _, ok := data[dbName]; !ok {
			data[dbName] = map[string][]DatabaseRecord{}
		}
		for _, tableName := range []string{"test_table_01", "test_table_02"} {
			var rows []DatabaseRecord
			var irows []interface{}
			for k := 1; k <= 2; k++ {
				rows = append(rows, generateRecord(k, 5, mark))
				irows = append(irows, generateRecord(k, 5, mark))
			}
			_, err := database.Database(dbName).Collection(tableName).InsertMany(context, irows)
			if err != nil {
				panic(err)
			}
			data[dbName][tableName] = rows
		}
	}
	return data
}

type DatabaseRecord struct {
	Datetime time.Time
	IntNum   int
	Str      string
}

func generateRecord(rowNum int, strLen int, strPrefix string) DatabaseRecord {
	return DatabaseRecord{
		Datetime: time.Now(),
		IntNum:   rowNum,
		Str:      strPrefix + testUtils.RandSeq(strLen),
	}
}

func getBackupNameFromExecOutput(output string) string {
	return strings.Trim(strings.Split(output, "FILE PATH: ")[1], " ")
}

func getBackupNamesFromExecOutput(output string) []string {
	re := regexp.MustCompile("stream_[0-9]{8}T[0-9]{6}Z")
	return re.FindAllString(output, -1)
}

func GetBackups(testContext *TestContextType, containerName string) ([]string, error) {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")
	backupListCommand := []string{WalgCliPath, "--config", WalgConfPath, "backup-list"}
	config := types.ExecConfig{
		AttachStderr: true,
		AttachStdout: true,
		Cmd:          backupListCommand,
	}
	responseIdExecCreate, err := testContext.DockerClient.ContainerExecCreate(testContext.Context, containerName, config)
	if err != nil {
		return []string{}, fmt.Errorf("error in getting backups: %v", err)
	}
	responseId, err := testContext.DockerClient.ContainerExecAttach(testContext.Context, responseIdExecCreate.ID, types.ExecStartCheck{})
	if err != nil {
		return []string{}, fmt.Errorf("error in getting backups: %v", err)
	}
	scanner := bufio.NewScanner(responseId.Reader)
	var response string
	for scanner.Scan() {
		response = response + scanner.Text()
	}
	return getBackupNamesFromExecOutput(response), nil
}

func MakeBackup(testContext *TestContextType, containerName string, cmdArgs string, creds UserConfiguration, envs []string) (string, error) {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")
	command := strings.Join([]string{WalgCliPath, "--config", WalgConfPath, "backup-push", cmdArgs}, " ")
	config := types.ExecConfig{
		AttachStderr: true,
		AttachStdout: true,
		Cmd:          []string{"bash", "-c", command},
		Env:          append(os.Environ(), envs...),
	}
	responseIdExecCreate, err := testContext.DockerClient.ContainerExecCreate(testContext.Context, containerName, config)
	if err != nil {
		return "", fmt.Errorf("error in making backup: %v", err)
	}
	responseId, err := testContext.DockerClient.ContainerExecAttach(testContext.Context, responseIdExecCreate.ID, types.ExecStartCheck{})
	if err != nil {
		return "", fmt.Errorf("error in making backup: %v", err)
	}
	scanner := bufio.NewScanner(responseId.Reader)
	var response string
	for scanner.Scan() {
		response = response + scanner.Text()
	}
	return getBackupNameFromExecOutput(response), nil
}

func DeleteBackup(testContext *TestContextType, containerName string, backupNum int) error {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")
	backupEntries, err := GetBackups(testContext, containerName)
	if err != nil {
		return err
	}
	command := []string{WalgCliPath, "--config", WalgConfPath, "delete", "before", backupEntries[backupNum+1], "--confirm"}
	_, err = RunCommandInContainer(testContext, containerName, command)
	return fmt.Errorf("error in deleting backup: %v", err)
}

func RunCommandInContainerWithOptions(testContext *TestContextType, containerName string, command []string, options types.ExecConfig) (string, error) {
	config := options
	config.AttachStderr = true
	config.AttachStdout = true
	config.Cmd = command
	responseIdExecCreate, err := testContext.DockerClient.ContainerExecCreate(testContext.Context, containerName, config)
	if err != nil {
		return "", fmt.Errorf("error in running command in container: %v", err)
	}
	responseId, err := testContext.DockerClient.ContainerExecAttach(testContext.Context, responseIdExecCreate.ID, types.ExecStartCheck{})
	if err != nil {
		return "", fmt.Errorf("error in running command in container: %v", err)
	}
	scanner := bufio.NewScanner(responseId.Reader)
	var response string
	for scanner.Scan() {
		response = response + scanner.Text()
	}
	return response, nil
}

func RunCommandInContainer(testContext *TestContextType, containerName string, command []string) (string, error) {
	return RunCommandInContainerWithOptions(testContext, containerName, command, types.ExecConfig{})
}

type UserData struct {
	Database   string
	Collection string
	Row        bson.M
}

func isSystemCollection(collectionName string) bool {
	return strings.HasPrefix(collectionName, "system.")
}

func GetAllUserData(context context.Context, connection *mongo.Client) ([]UserData, error) {
	var userData []UserData
	dbNames, err := connection.ListDatabaseNames(context, bson.M{})
	if err != nil {
		return []UserData{}, fmt.Errorf("error in getting data from mongodb: %v", err)
	}
	sort.Strings(dbNames)
	for _, dbName := range dbNames {
		tables, err := connection.Database(dbName, &options.DatabaseOptions{}).ListCollectionNames(context, bson.M{})
		if err != nil {
			return []UserData{}, fmt.Errorf("error in getting data from mongodb: %v", err)
		}
		sort.Strings(tables)
		for _, table := range tables {
			if isSystemCollection(table) {
				continue
			}
			if dbName == "local" || dbName == "config" {
				continue
			}
			findOptions := options.Find()
			findOptions.SetSort(map[string]int{"_id": 1})
			cur, err := connection.Database(dbName, &options.DatabaseOptions{}).Collection(table).Find(context, bson.M{}, findOptions)
			if err != nil {
				return []UserData{}, fmt.Errorf("error in getting data from mongodb: %v", err)
			}
			for cur.Next(context) {
				var row bson.M
				err = cur.Decode(&row)
				if err != nil {
					return []UserData{}, fmt.Errorf("error in getting data from mongodb: %v", err)
				}
				userData = append(userData, UserData{
					Database:   dbName,
					Collection: table,
					Row:        row,
				})
			}
			err = cur.Close(context)
			if err != nil {
				return []UserData{}, fmt.Errorf("error in getting data from mongodb: %v", err)
			}
		}
	}
	return userData, nil
}

func checkRsInitialized(context context.Context, connection *mongo.Client) bool {
	response := connection.Database("admin").RunCommand(context, "replSetGetStatus")
	return response != nil
}

func StepEnsureRsInitialized(testContext *TestContextType, containerName string) error {
	var response string
	var err error
	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		command := []string{"mongo", "--host", "localhost", "--quiet", "--norc", "--port", "27018", "--eval", "rs.status()"}
		response, err = RunCommandInContainer(testContext, containerName, command)
		if strings.Contains(response, "myState") {
			return nil
		}
		if strings.Contains(response, "NotYetInitialized") {
			ncmd := []string{"mongo", "--host", "localhost", "--quiet", "--norc", "--port", "27018", "--eval", "rs.initiate()"}
			_, err = RunCommandInContainer(testContext, containerName, ncmd)
		} else if strings.Contains(response, "Unauthorized") {
			creds := UserConfiguration{
				Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
				Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
				Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
				Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
			}
			var connection *mongo.Client
			connection, err = EnvDBConnectWithCreds(testContext, containerName, creds)
			if checkRsInitialized(testContext.Context, connection) {
				return nil
			}
		}
	}
	return fmt.Errorf("replset was not initialized: %s;\n and finished with last error: %v", response, err)
}

func RestoreBackupById(testContext *TestContextType, containerName string, backupNum int) error {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")
	backupEntries, err := GetBackups(testContext, containerName)
	if err != nil {
		return fmt.Errorf("error in restoring backup by id: %v", err)
	}
	walgCommand := []string{WalgCliPath, "--config", WalgConfPath, "backup-fetch", backupEntries[len(backupEntries)-backupNum-1]}
	mongoCommand := []string{"|", "mongorestore", "--archive", "--uri=\"mongodb://admin:password@127.0.0.1:27018\""}
	command := strings.Join(append(walgCommand, mongoCommand...), " ")
	_, err = RunCommandInContainer(testContext, containerName, []string{"bash", "-c", command})
	return err
}

func MongoPurgeAllBackups(testContext *TestContextType, containerName string) error {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")
	command := []string{WalgCliPath, "--config", WalgConfPath, "delete", "everything", "--confirm"}
	_, err := RunCommandInContainer(testContext, containerName, command)
	return err
}

func MongoPurgeBackups(testContext *TestContextType, containerName string, keepNumber int) error {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")
	command := []string{WalgCliPath, "--config", WalgConfPath, "delete", "retain", strconv.Itoa(keepNumber), "--confirm"}
	_, err := RunCommandInContainer(testContext, containerName, command)
	return err
}

func MongoPurgeBackupsAfterBackupId(testContext *TestContextType, containerName string,
	keepNumber int, afterBackupNum int) error {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")

	backupEntries, err := GetBackups(testContext, containerName)
	if err != nil {
		return fmt.Errorf("error in restoring backup by id: %v", err)
	}

	command := []string{WalgCliPath, "--config", WalgConfPath, "delete",
		"retain_after", strconv.Itoa(keepNumber), backupEntries[len(backupEntries)-afterBackupNum-1], "--confirm"}

	_, err = RunCommandInContainer(testContext, containerName, command)
	return err
}

func MongoPurgeBackupsAfterTime(testContext *TestContextType, containerName string,
	keepNumber int, timeLine time.Time) error {
	WalgCliPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CLIENT_PATH")
	WalgConfPath := testUtils.GetVarFromEnvList(testContext.Env, "WALG_CONF_PATH")
	command := []string{WalgCliPath, "--config", WalgConfPath, "delete",
		"retain_after", strconv.Itoa(keepNumber), timeLine.Format(time.RFC3339), "--confirm"}

	_, err := RunCommandInContainer(testContext, containerName, command)
	return err
}
