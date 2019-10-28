package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var WalgCliPath = "/usr/bin/wal-g"
var WalgConfPath = "/home/.walg.json"
var WalgDefaultArgs = ""

func connectHP(host string, port uint16) *mongo.Client {
	uri := fmt.Sprintf("mongodb://%s:%d/?connect=direct", host, port)
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	err = client.Connect(context.Background())
	if err != nil {
		panic(err)
	}
	return client
}

func connect(user string, password string, dbname string, host string, port uint16) *mongo.Client {
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d/%s?connect=direct&authMechanism=SCRAM-SHA-1", user, password, host, port, dbname)
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	err = client.Connect(context.Background())
	if err != nil {
		panic(err)
	}
	return client
}

func EnvDBConnect(testContext *TestContextType, nodeName string) *mongo.Client {
	dbMongoPort, err := strconv.Atoi(GetVarFromEnvList(testContext.Env, "DB_MONGO_PORT"))
	if err != nil {
		panic(err)
	}
	dbHost := GetDockerContainer(testContext, nodeName)
	host, port := getExposedPort(*dbHost, uint16(dbMongoPort))
	conn := connectHP(
		host,
		port)
	return conn
}

func EnvDBConnectWithCreds(testContext *TestContextType, nodeName string, creds UserConfiguration) * mongo.Client {
	dbMongoPort, err := strconv.Atoi(GetVarFromEnvList(testContext.Env, "DB_MONGO_PORT"))
	if err != nil {
		panic(err)
	}
	dbHost := GetDockerContainer(testContext, nodeName)
	host, port := getExposedPort(*dbHost, uint16(dbMongoPort))
	conn := connect(
		creds.Username,
		creds.Password,
		creds.Dbname,
		host,
		port)
	return conn
}


func FillWithData(database *mongo.Client, mark string)  map[string]map[string][]DatabaseRecord {
	var data = make(map[string]map[string][]DatabaseRecord, 0)
	for i := 1; i <= 2; i++ {
		dbName := fmt.Sprintf("test_db_%02d", i)
		if _, ok := data[dbName]; !ok {
			data[dbName] = map[string][]DatabaseRecord{}
		}
		for j := 1; j <= 2; j++ {
			var rows []DatabaseRecord
			var irows []interface{}
			tableName := fmt.Sprintf("test_table_%02d", j)
			for k := 1; k <= 2; k++ {
				rows = append(rows, generateRecord(k, 5, mark))
				irows = append(irows, generateRecord(k, 5, mark))
			}
			_, err := database.Database(dbName).Collection(tableName).InsertMany(context.Background(), irows)
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
		Str:      strPrefix + RandSeq(strLen),
	}
}

func getBackupNameFromExecOutput(output string) string {
	return strings.Trim(strings.Split(output, "FILE PATH: ")[1], " ")
}

func getBackupNamesFromExecOutput(output string) []string {
	re := regexp.MustCompile("stream_[0-9]{8}T[0-9]{6}Z")
	return re.FindAllString(output, -1)
}

func GetBackups(testContext *TestContextType, containerName string) []string {
	backupListCommand := []string{WalgCliPath, "--config", WalgConfPath, "backup-list"}
	config :=  types.ExecConfig{
		AttachStderr: true,
		AttachStdout: true,
		Cmd: backupListCommand,
	}
	responseIdExecCreate, err := testContext.DockerClient.ContainerExecCreate(context.Background(), containerName, config)
	if err != nil {
		panic(err)
	}
	responseId, err := testContext.DockerClient.ContainerExecAttach(context.Background(), responseIdExecCreate.ID, types.ExecStartCheck{})
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(responseId.Reader)
	var response string
	for scanner.Scan() {
		response = response + scanner.Text()
	}
	return getBackupNamesFromExecOutput(response)
}

func MakeBackup(testContext *TestContextType, containerName string, cmdArgs string) string {
	backupCommand := []string{WalgCliPath, "--config", WalgConfPath, "stream-push" , cmdArgs}
	config :=  types.ExecConfig{
		AttachStderr: true,
		AttachStdout: true,
		Cmd: backupCommand,
	}
	responseIdExecCreate, err := testContext.DockerClient.ContainerExecCreate(context.Background(), containerName, config)
	if err != nil {
		panic(err)
	}
	responseId, err := testContext.DockerClient.ContainerExecAttach(context.Background(), responseIdExecCreate.ID, types.ExecStartCheck{})
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(responseId.Reader)
	var response string
	for scanner.Scan() {
		response = response + scanner.Text()
	}
	return getBackupNameFromExecOutput(response)
}

func DeleteBackup(testContext *TestContextType, containerName string, backupNum int) {
	backupEntries := GetBackups(testContext, containerName)
	fmt.Printf("\n\n%+v\n\n", backupEntries)
	command := []string{WalgCliPath, "--config", WalgConfPath, "delete", "before", backupEntries[backupNum + 1], "--confirm"}
	RunCommandInContainer(testContext, containerName, command)
}

func RunCommandInContainerWithOptions(testContext *TestContextType, containerName string, command []string, options types.ExecConfig) string {
	config := options
	config.AttachStderr = true
	config.AttachStdout = true
	config.Cmd = command
	responseIdExecCreate, err := testContext.DockerClient.ContainerExecCreate(context.Background(), containerName, config)
	if err != nil {
		panic(err)
	}
	responseId, err := testContext.DockerClient.ContainerExecAttach(context.Background(), responseIdExecCreate.ID, types.ExecStartCheck{})
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(responseId.Reader)
	var response string
	for scanner.Scan() {
		response = response + scanner.Text()
	}
	return response
}

func RunCommandInContainer(testContext *TestContextType, containerName string, command []string) string {
	return RunCommandInContainerWithOptions(testContext, containerName, command, types.ExecConfig{})
}

type UserData struct {
	Database   string
	Collection string
	Row        bson.M
}

func GetAllUserData(connection *mongo.Client) []UserData {
	var userData []UserData
	dbNames, err := connection.ListDatabaseNames(context.Background(), bson.M{})
	if err != nil {
		panic(err)
	}
	for _, dbName := range dbNames {
		tables, err := connection.Database(dbName, &options.DatabaseOptions{}).ListCollectionNames(context.Background(), bson.M{})
		if err != nil {
			panic(err)
		}
		for _, table := range tables {
			if dbName == "local" {
				continue
			}
			cur, err := connection.Database(dbName, &options.DatabaseOptions{}).Collection(table).Find(context.Background(), bson.M{})
			if err != nil {
				panic(err)
			}
			for cur.Next(context.Background()) {
				var row bson.M
				err = cur.Decode(&row)
				if err != nil {
					panic(err)
				}
				userData = append(userData, UserData{
					Database:   dbName,
					Collection: table,
					Row:        row,
				})
			}
			err = cur.Close(context.Background())
			if err != nil {
				panic(err)
			}
		}
	}
	return userData
}

func checkRsInitialized(connection *mongo.Client) bool {
	// example from here https://github.com/stefanprodan/mongo-swarm/blob/4c80693d3a6cf74282d1bd249f35cf4bded13cc1/bootstrap/replicaset.go
	response := connection.Database("admin").RunCommand(context.Background(), "replSetGetStatus")
	return response != nil
}

func StepEnsureRsInitialized(testContext *TestContextType, containerName string) {
	var response string
	for i := 0; i < 15; i++ {
		command := []string{"mongo", "--host", "localhost", "--quiet", "--norc", "--port", "27018", "--eval", "rs.status()"}
		response = RunCommandInContainer(testContext, containerName, command)
		if strings.Contains(response, "myState") {
			return
		}
		if strings.Contains(response, "NotYetInitialized") {
			ncmd := []string{"mongo", "--host", "localhost", "--quiet", "--norc", "--port", "27018", "--eval", "rs.initiate()"}
			_ = RunCommandInContainer(testContext, containerName, ncmd)
		} else if strings.Contains(response, "Unauthorized") {
			creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
			connection := EnvDBConnectWithCreds(testContext, containerName, creds)
			if checkRsInitialized(connection) {
				return
			}
		}
	}
	panic(fmt.Errorf("replset was not initialized: %s", response))
}

func restoreBackupById(testContext *TestContextType, containerName string, backupNum int) {
	backupEntries := GetBackups(testContext, containerName)
	command := []string{WalgCliPath, "--config", WalgConfPath, "stream-fetch", backupEntries[backupNum]}
	RunCommandInContainer(testContext, containerName, command)
}

func MongoPurgeBackups(testContext *TestContextType, containerName string, keepNumber int) {
	command := []string{WalgCliPath, "--config", WalgConfPath, "delete", "retain", strconv.Itoa(keepNumber), "--confirm"}
	_ = RunCommandInContainer(testContext, containerName, command)
}
