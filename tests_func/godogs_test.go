package functest

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/colors"
	"github.com/DATA-DOG/godog/gherkin"
	"github.com/docker/docker/client"
	testHelper "github.com/wal-g/wal-g/tests_func/helpers"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testContext *testHelper.TestContextType

func FeatureContext(s *godog.Suite) {
	testContext = &testHelper.TestContextType{}
	testContext.TestData = make(map[string]map[string]map[string][]testHelper.DatabaseRecord)
	testContext.AuxData.Timestamps = make(map[int]time.Time)
	testContext.Context = context.Background()

	s.BeforeFeature(func(feature *gherkin.Feature) {
		testContext = &testHelper.TestContextType{}
		testContext.TestData = make(map[string]map[string]map[string][]testHelper.DatabaseRecord)
		testContext.AuxData.Timestamps = make(map[int]time.Time)
		testContext.Context = context.Background()

		var err error
		testContext.Env, err = testHelper.GetTestEnv(testContext)
		if err == nil {
			var containerNames []string
			testId := testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")
			for _, envLine := range testContext.Env {
				key, value := testUtils.SplitEnvLine(envLine)
				if strings.HasSuffix(key, "_WORKER") {
					containerNames = append(containerNames, fmt.Sprintf("%s.test_net_%s", value, testId))
				}
			}
			testContext.DockerClient, err = client.NewClientWithOpts(client.WithVersion("1.38"))
			for _, container := range containerNames {
				_, err = testHelper.GetDockerContainer(testContext, container)
				if err != nil {
					break
				}
			}
			if err == nil {
				nodeName := fmt.Sprintf("%s.test_net_%s", testUtils.GetVarFromEnvList(testContext.Env, "MONGO_HOST_01_WORKER"), testId)
				if testHelper.MongoPurgeAllBackups(testContext, nodeName) == nil {
					fmt.Printf("\nUsing existing docker containers\n")
					return
				}
			}
		}

		err = SetupStaging(testContext)
		if err != nil {
			log.Fatalln(err)
		}
		err = BuildBase(testContext)
		if err != nil {
			log.Fatalln(err)
		}
		err = Start(testContext)
		if err != nil {
			log.Fatalln(err)
		}
		err = testUtils.WriteEnvFile(testContext.Env, testUtils.GetVarFromEnvList(testContext.Env, "ENV_FILE"))
		if err != nil {
			log.Fatalln(err)
		}
	})

	s.AfterFeature(func(feature *gherkin.Feature) {

	})

	s.BeforeStep(func(s *gherkin.Step) {

	})

	s.AfterStep(func(s *gherkin.Step, err error) {
	})

	s.Step(`^a working mongodb on mongodb(\d+)$`, testMongodbConnect)
	s.Step(`^a configured s3 on minio(\d+)$`, configureS3OnMinio)
	s.Step(`^mongodb replset initialized on mongodb(\d+)$`, replsetinitiateOnMongodb)
	s.Step(`^mongodb role is primary on mongodb(\d+)$`, testMongodbPrimaryRole)
	s.Step(`^mongodb auth initialized on mongodb(\d+)$`, authenticateOnMongodb)
	s.Step(`^mongodb(\d+) has test mongodb data test(\d+)$`, fillMongodbWithTestData)
	s.Step(`^we create mongodb(\d+) backup$`, createMongodbBackup)
	s.Step(`^we create mongodb(\d+) backup with user data$`, createMongodbBackupWithUserData)
	s.Step(`^we got (\d+) backup entries of mongodb(\d+)$`, testBackupEntriesOfMongodb)
	s.Step(`^we put empty backup via minio(\d+)$`, putEmptyBackupViaMinio)
	s.Step(`^we delete backups retain (\d+) via mongodb(\d+)$`, deleteBackupsRetainViaMongodb)
	s.Step(`^we check if empty backups were purged via minio(\d+)$`, testEmptyBackupsViaMinio)

	s.Step(`^we delete #(\d+) backup via mongodb(\d+)$`, deleteBackupViaMongodb)
	s.Step(`^we restore #(\d+) backup to mongodb(\d+)$`, restoreBackupToMongodb)
	s.Step(`^we got same mongodb data at mongodb(\d+) mongodb(\d+)$`, testEqualMongodbDataAtMongodbs)
	s.Step(`^we ensure mongodb(\d+) #(\d+) backup metadata contains$`, mongodbBackupMetadataContainsUserData)

	s.Step(`^we delete backups retain (\d+) after #(\d+) backup via mongodb(\d+)$`, deleteBackupsRetainAfterBackupViaMongodb)
	s.Step(`^we delete backups retain (\d+) after #(\d+) timestamp via mongodb(\d+)$`, deleteBackupsRetainAfterTimeViaMongodb)
	s.Step(`^we create timestamp #(\d+) via mongodb(\d+)$`, createTimestamp)
	s.Step(`^we wait for (\d+) seconds$`, wait)
}

func wait(cnt int) error {
	time.Sleep(time.Duration(cnt * int(time.Second)))
	return nil
}

func createTimestamp(timestampId int, mongodbId int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	command := []string{"date", "-u", `+%Y-%m-%dT%H:%M:%SZ`}
	response, err := testHelper.RunCommandInContainer(testContext, nodeName, command)
	if err != nil {
		return fmt.Errorf("cannot create timestamp: %v", err)
	}
	timeStr := strings.Trim(response, " \n\t"+string([]byte{0, 1, 21}))
	timeLine, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		return fmt.Errorf("cannot create timestamp: %v", err)
	}
	testContext.AuxData.Timestamps[timestampId] = timeLine
	return nil
}

func deleteBackupsRetainAfterTimeViaMongodb(retainCount int, timestampId int, mongodbId int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.MongoPurgeBackupsAfterTime(testContext, containerName, retainCount, testContext.AuxData.Timestamps[timestampId])
}

func deleteBackupsRetainAfterBackupViaMongodb(retainCount int, afterBackupId int, mongodbId int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.MongoPurgeBackupsAfterBackupId(testContext, containerName, retainCount, afterBackupId)
}

func mongodbBackupMetadataContainsUserData(mongodbId int, backupId int, data *gherkin.DocString) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	backupList, err := testHelper.GetBackups(testContext, nodeName)
	if err != nil {
		return err
	}
	if len(backupList)-1-backupId < 0 {
		return fmt.Errorf("cannot get %dth backup - there are only %d", backupId, len(backupList))
	}
	backup := backupList[len(backupList)-1-backupId]
	path := fmt.Sprintf("/export/dbaas/mongodb-backup/test_uuid/test_mongodb/basebackups_005/%s_backup_stop_sentinel.json", backup)
	cmd := []string{"cat", path}
	jsonString, _ := testHelper.RunCommandInContainer(testContext, fmt.Sprintf("minio01.test_net_%s", testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")), cmd)
	jsonString = jsonString[strings.Index(jsonString, "{"):]

	var rawUserData map[string]*json.RawMessage
	err = json.Unmarshal([]byte(jsonString), &rawUserData)
	if err != nil {
		return err
	}

	var userData map[string]*json.RawMessage
	err = json.Unmarshal(*rawUserData["UserData"], &userData)
	if err != nil {
		return err
	}

	var labels map[string]string
	err = json.Unmarshal(*userData["labels"], &labels)
	if err != nil {
		return err
	}

	content := getMakeBackupContentFromDocString(data)

	if !reflect.DeepEqual(labels, content["labels"]) {
		return fmt.Errorf("error: expected labels don't equal to existing in backup")
	}

	return nil
}

func testMongodbConnect(mongodbId int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	const timeoutInterval = 100 * time.Millisecond
	for i := 0; i < 25; i++ {
		connection, _ := testHelper.EnvDBConnect(testContext, nodeName)
		err := connection.Database(nodeName).Client().Ping(testContext.Context, nil)
		if err == nil {
			return nil
		}
		time.Sleep(timeoutInterval)
	}
	return fmt.Errorf("cannot connect to %s", nodeName)
}

func configureS3OnMinio(minioId int) error {
	nodeName := fmt.Sprintf("minio%02d.test_net_%s", minioId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	container, err := testHelper.GetDockerContainer(testContext, nodeName)
	if err != nil {
		return err
	}
	err = testHelper.ConfigureS3(testContext, container)
	if err != nil {
		return err
	}
	return nil
}

func replsetinitiateOnMongodb(mongodbId int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	err := testHelper.StepEnsureRsInitialized(testContext, nodeName)
	if err != nil {
		return err
	}
	time.Sleep(3 * time.Second)
	return nil
}

func testMongodbPrimaryRole(mongodbId int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	creds := testHelper.UserConfiguration{
		Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
		Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
		Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
		Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
	}
	connection, err := testHelper.EnvDBConnectWithCreds(testContext, nodeName, creds)
	if err != nil {
		return err
	}
	smth := connection.Ping(testContext.Context, readpref.Primary())
	return smth
}

func authenticateOnMongodb(mongodbId int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	creds := testHelper.UserConfiguration{
		Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
		Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
		Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
		Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
	}
	roles := "["
	for _, value := range creds.Roles {
		roles = roles + "'" + value + "', "
	}
	roles = strings.Trim(roles, ", ") + "]"
	command := []string{"mongo", "--host", "localhost", "--quiet", "--norc", "--port", "27018", "--eval",
		fmt.Sprintf("db.createUser({user: '%s', pwd: '%s', roles: %s})",
			creds.Username,
			creds.Password,
			roles),
		testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME")}
	response, err := testHelper.RunCommandInContainer(testContext, nodeName, command)
	if err != nil {
		return err
	}
	if strings.Contains(response, "command createUser requires authentication") {
		command = append(command, "-u", creds.Username, "-p", creds.Password)
		response, err = testHelper.RunCommandInContainer(testContext, nodeName, command)
		if err != nil {
			return err
		}
	}
	if !strings.Contains(response, "Successfully added user") &&
		!strings.Contains(response, "not authorized on admin") &&
		!strings.Contains(response, "already exists") {
		return fmt.Errorf("can not initialize auth: %s", response)
	}
	return nil
}

func fillMongodbWithTestData(mongodbId, testId int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	testName := fmt.Sprintf("test%02d", testId)
	creds := testHelper.UserConfiguration{
		Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
		Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
		Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
		Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
	}
	conn, err := testHelper.EnvDBConnectWithCreds(testContext, nodeName, creds)
	if err != nil {
		return err
	}
	data := testHelper.FillWithData(testContext.Context, conn, testName)
	testContext.TestData["test"+string(testId)] = data
	return nil
}

func createMongodbBackup(mongodbId int) error {
	var cmdArgs = ""
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	creds := testHelper.UserConfiguration{
		Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
		Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
		Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
		Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
	}
	currentBackupId, err := testHelper.MakeBackup(testContext, containerName, cmdArgs, creds, []string{})
	if err != nil {
		return err
	}
	testContext.SafeStorage.CreatedBackupNames = append(testContext.SafeStorage.CreatedBackupNames, currentBackupId)
	return nil
}

func getMakeBackupContentFromDocString(content *gherkin.DocString) map[string]map[string]string {
	var lines = strings.Split(content.Content, "\n")
	res := make(map[string]map[string]string, 0)
	var outerKey string
	for _, line := range lines {
		if !strings.HasPrefix(line, " ") {
			outerKey = strings.Trim(line, " :")
			res[outerKey] = map[string]string{}
		} else {
			values := strings.Split(strings.Trim(line, " "), " ")
			innerKey := strings.Trim(values[0], ": ")
			value := strings.Trim(values[1], ": ")
			res[outerKey][innerKey] = value
		}
	}
	return res
}

func createMongodbBackupWithUserData(mongodbId int, data *gherkin.DocString) error {
	var cmdArgs = ""
	var envs []string
	if data != nil {
		content := getMakeBackupContentFromDocString(data)
		var args []string
		if labels, ok := content["labels"]; ok {
			for key, value := range labels {
				args = append(args, fmt.Sprintf(`"%s": "%s"`, key, value))
			}
		}
		envs = append(envs, fmt.Sprintf(`WALG_SENTINEL_USER_DATA={"labels": {%s}}`, strings.Join(args, ", ")))
	}
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	creds := testHelper.UserConfiguration{
		Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
		Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
		Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
		Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
	}
	currentBackupId, err := testHelper.MakeBackup(testContext, containerName, cmdArgs, creds, envs)
	if err != nil {
		return err
	}
	testContext.SafeStorage.CreatedBackupNames = append(testContext.SafeStorage.CreatedBackupNames, currentBackupId)
	return nil
}

func testBackupEntriesOfMongodb(backupCount, mongodbId int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	backupNames, err := testHelper.GetBackups(testContext, containerName)
	if err != nil {
		return err
	}
	if len(backupNames) != backupCount {
		return fmt.Errorf("expected %d number of backups, but found %d", backupCount, len(backupNames))
	}
	return nil
}

func putEmptyBackupViaMinio(minioId int) error {
	containerName := fmt.Sprintf("minio%02d.test_net_%s", minioId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	backupName := "20010203T040506"
	bucketName := testUtils.GetVarFromEnvList(testContext.Env, "S3_BUCKET")
	backupRootDir := testUtils.GetVarFromEnvList(testContext.Env, "WALG_S3_PREFIX")
	backupDir := "/export/" + bucketName + "/" + backupRootDir + "/" + backupName
	backupDumpPath := filepath.Join(backupDir, "mongodump.archive")
	testContext.SafeStorage.NometaBackupNames = append(testContext.SafeStorage.NometaBackupNames, backupName)
	_, err := testHelper.RunCommandInContainer(testContext, containerName, []string{"mkdir", "-p", backupDir})
	if err != nil {
		return err
	}
	_, err = testHelper.RunCommandInContainer(testContext, containerName, []string{"touch", backupDumpPath})
	if err != nil {
		return err
	}
	return nil
}

func deleteBackupsRetainViaMongodb(retainCount, mongodbId int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.MongoPurgeBackups(testContext, containerName, retainCount)
}

func testEmptyBackupsViaMinio(minioId int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", minioId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	bucketName := testUtils.GetVarFromEnvList(testContext.Env, "S3_BUCKET")
	backupRootDir := testUtils.GetVarFromEnvList(testContext.Env, "WALG_S3_PREFIX")
	backupNames := testContext.SafeStorage.NometaBackupNames
	for _, backupName := range backupNames {
		backupDir := filepath.Join("/export", bucketName, backupRootDir, backupName)
		_, err := testHelper.RunCommandInContainer(testContext, containerName, []string{"ls", backupDir})
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteBackupViaMongodb(backupId, mongodbId int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.DeleteBackup(testContext, containerName, backupId)
}

func restoreBackupToMongodb(backupId, mongodbId int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", mongodbId, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.RestoreBackupById(testContext, containerName, backupId)
}

func testEqualMongodbDataAtMongodbs(mongodbId1, mongodbId2 int) error {
	creds := testHelper.UserConfiguration{
		Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
		Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
		Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
		Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
	}
	containerName1 := fmt.Sprintf("mongodb%02d", mongodbId1) + ".test_net_" + testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")
	containerName2 := fmt.Sprintf("mongodb%02d", mongodbId2) + ".test_net_" + testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")

	connection1, err := testHelper.EnvDBConnectWithCreds(testContext, containerName1, creds)
	if err != nil {
		return err
	}
	connection2, err := testHelper.EnvDBConnectWithCreds(testContext, containerName2, creds)
	if err != nil {
		return err
	}

	var userData [][]testHelper.UserData
	rowsData1, err := testHelper.GetAllUserData(testContext.Context, connection1)
	if err != nil {
		return err
	}
	rowsData2, err := testHelper.GetAllUserData(testContext.Context, connection2)
	if err != nil {
		return err
	}

	userData = append(userData, rowsData1)
	userData = append(userData, rowsData2)

	if !reflect.DeepEqual(rowsData1, rowsData2) {
		return fmt.Errorf("expected the same data in %s and %s", containerName1, containerName2)
	}
	return nil
}

var opt = godog.Options{
	Output: colors.Colored(os.Stdout),
	Format: "progress",
}

func init() {
	godog.BindFlags("godog.", flag.CommandLine, &opt)
}

func TestMain(m *testing.M) {
	flag.Parse()
	opt.Paths = flag.Args()

	status := godog.RunWithOptions("godogs", func(s *godog.Suite) {
		FeatureContext(s)
	}, opt)

	if st := m.Run(); st > status {
		status = st
	}

	err := testHelper.ShutdownContainers(testContext)
	if err != nil {
		log.Fatalln(err)
	}
	err = testHelper.ShutdownNetwork(testContext)
	if err != nil {
		log.Fatalln(err)
	}
	err = os.RemoveAll("./staging/images/")
	if err != nil {
		log.Fatalln(err)
	}
	err = os.Remove(testUtils.GetVarFromEnvList(testContext.Env, "ENV_FILE"))

	os.Exit(status)
}
