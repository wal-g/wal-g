package functest

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/colors"
	"github.com/DATA-DOG/godog/gherkin"

	testConf "github.com/wal-g/wal-g/tests_func/config"
	testHelper "github.com/wal-g/wal-g/tests_func/helpers"
	testLoad "github.com/wal-g/wal-g/tests_func/load"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"

	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var testContext *testHelper.TestContextType

func FeatureContext(s *godog.Suite) {
	s.BeforeFeature(func(feature *gherkin.Feature) {
		testContext.TestData = make(map[string]map[string]map[string][]testHelper.DatabaseRecord)
		testContext.AuxData.Timestamps = make(map[string]time.Time)
		testContext.AuxData.DatabaseSnap = make(map[string][]testHelper.UserData)
		if err := StartRecreate(testContext); err != nil {
			log.Fatalln(err)
		}
	})

	s.BeforeSuite(func() {
		stagingPath := testConf.Env["STAGING_DIR"]
		envFilePath := testConf.Env["ENV_FILE"]
		newEnv := !EnvExists(envFilePath)
		if newEnv {
			if err := SetupEnv(envFilePath, stagingPath); err != nil {
				log.Fatalln(err)
			}
		}
		env, err := ReadEnv(envFilePath)
		if err != nil {
			log.Fatalln(err)
		}
		testContext.Env = testUtils.MergeEnvs(testUtils.ParseEnvLines(os.Environ()), env)

		if newEnv {
			if err := SetupStaging(testContext); err != nil {
				log.Fatalln(err)
			}

			if err := BuildBase(testContext); err != nil {
				log.Fatalln(err)
			}
		}

	})

	s.AfterFeature(func(feature *gherkin.Feature) {

	})

	s.BeforeStep(func(s *gherkin.Step) {

	})

	s.AfterStep(func(s *gherkin.Step, err error) {
	})

	s.Step(`^a working mongodb on ([^\s]*)$`, testMongodbConnect)
	s.Step(`^a configured s3 on ([^\s]*)$`, configureS3OnMinio)
	s.Step(`^mongodb replset initialized on ([^\s]*)$`, replsetinitiateOnMongodb)
	s.Step(`^mongodb role is primary on ([^\s]*)$`, testMongodbPrimaryRole)
	s.Step(`^mongodb auth initialized on ([^\s]*)$`, authenticateOnMongodb)
	s.Step(`^([^\s]*) has test mongodb data test(\d+)$`, fillMongodbWithTestData)
	s.Step(`^we create ([^\s]*) backup$`, createMongodbBackup)
	s.Step(`^we create ([^\s]*) backup with user data$`, createMongodbBackupWithUserData)
	s.Step(`^we got (\d+) backup entries of ([^\s]*)$`, testBackupEntriesOfMongodb)
	s.Step(`^we put empty backup via ([^\s]*)$`, putEmptyBackupViaMinio)
	s.Step(`^we delete backups retain (\d+) via ([^\s]*)$`, deleteBackupsRetainViaMongodb)
	s.Step(`^we check if empty backups were purged via ([^\s]*)$`, testEmptyBackupsViaMinio)

	s.Step(`^we delete #(\d+) backup via ([^\s]*)$`, deleteBackupViaMongodb)
	s.Step(`^we restore #(\d+) backup to ([^\s]*)$`, restoreBackupToMongodb)
	s.Step(`^we got same mongodb data at ([^\s]*) ([^\s]*)$`, testEqualMongodbDataAtMongodbs)
	s.Step(`^we ensure ([^\s]*) #(\d+) backup metadata contains$`, mongodbBackupMetadataContainsUserData)

	s.Step(`^we delete backups retain (\d+) after #(\d+) backup via ([^\s]*)$`, deleteBackupsRetainAfterBackupViaMongodb)
	s.Step(`^we delete backups retain (\d+) after "([^"]*)" timestamp via ([^\s]*)$`, deleteBackupsRetainAfterTimeViaMongodb)
	s.Step(`^we create timestamp "([^"]*)" via ([^\s]*)$`, createTimestamp)
	s.Step(`^we wait for (\d+) seconds$`, wait)
	s.Step(`^oplog archive is on ([^\s]*)$`, sendOplogOn)
	s.Step(`^we load ([^\s]*) with "([^"]*)" config$`, loadMongodbWithConfig)
	s.Step(`^we save ([^\s]*) data "([^"]*)"$`, saveMongodbData)
	s.Step(`^([^\s]*) has no data$`, cleanMongoDb)
	s.Step(`^we restore from "([^"]*)" timestamp to "([^"]*)" timestamp to ([^\s]*)$`, mongodbRestoreOplog)
	s.Step(`^we have same data in "([^"]*)" and "([^"]*)"$`, sameDataCheck)

}

func sameDataCheck(dataId1, dataId2 string) error {
	if data1, ok := testContext.AuxData.DatabaseSnap[dataId1]; ok {
		if data2, ok := testContext.AuxData.DatabaseSnap[dataId2]; ok {
			if !reflect.DeepEqual(data1, data2) {
				if testUtils.ParseEnvLines(os.Environ())["DEBUG"] != "" {
					fmt.Printf("\nData check failed:\nData %s:\n %+v\n\nData %s:\n %+v\n",
						dataId1, data1, dataId2, data2)
				}
				return fmt.Errorf("expected the same data in %s and %s", dataId1, dataId2)
			}
			return nil
		}
		return fmt.Errorf("no data is saved for with id %s", dataId2)
	}
	return fmt.Errorf("no data is saved for with id %s", dataId1)
}

func mongodbRestoreOplog(timestampIdFrom, timestampIdUntil, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	storageName := fmt.Sprintf("minio%02d.test_net_%s", 1, testContext.Env["TEST_ID"])
	return testHelper.MongoOplogFetch(testContext, containerName, storageName, timestampIdFrom, timestampIdUntil)
}

func cleanMongoDb(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	connection, _ := testHelper.AdminConnect(testContext, containerName)

	dbNames, err := connection.ListDatabaseNames(testContext.Context, bson.M{})
	if err != nil {
		return fmt.Errorf("error in getting data from mongodb: %v", err)
	}

	for _, dbName := range dbNames {
		if dbName == "local" || dbName == "config" || dbName == "admin" {
			continue
		}
		err := connection.Database(dbName).Drop(testContext.Context)
		if err != nil {
			return err
		}
	}

	return nil
}

func saveMongodbData(nodeName, dataId string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	connection, err := testHelper.AdminConnect(testContext, containerName)
	if err != nil {
		return err
	}
	rowsData, err := testHelper.GetAllUserData(testContext.Context, connection)
	if err != nil {
		return err
	}
	if testUtils.ParseEnvLines(os.Environ())["DEBUG"] != "" {
		fmt.Printf("\nSaving data from %s to data %s:\n %+v\n", containerName, dataId, rowsData)
	}
	testContext.AuxData.DatabaseSnap[dataId] = rowsData
	return nil
}

func loadMongodbWithConfig(nodeName, configFile string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])

	patrons, err := testLoad.GeneratePatronsFromFile(configFile)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8050*time.Millisecond)
	defer func() {
		for _, patronFile := range patrons {
			if err := os.RemoveAll(patronFile + ".json"); err != nil {
			}
		}
		cancel()
	}()

	for _, patronFile := range patrons {
		err := func() error {
			f, err := os.Open(patronFile + ".json")
			if err != nil {
				return fmt.Errorf("cannot read patron: %v", err)
			}
			defer f.Close()
			roc, _, err := testLoad.ReadRawMongoOps(ctx, f, 3)
			if err != nil {
				return err
			}
			cli, err := testHelper.AdminConnect(testContext, containerName)
			if err != nil {
				return err
			}
			cmdc, erc := testLoad.MakeMongoOps(ctx, cli, roc)
			// put all results somewhere for stats maybe
			c := testLoad.RunMongoOpFuncs(ctx, cmdc, 3, 3)
			for _ = range c {
			}
			for err := range erc {
				if err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func sendOplogOn(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	return testHelper.MongoOplogPush(testContext, containerName)
}

func wait(cnt int) error {
	time.Sleep(time.Duration(cnt * int(time.Second)))
	return nil
}

func createTimestamp(timestampId, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	command := []string{"date", "-u", `+%Y-%m-%dT%H:%M:%SZ`}
	response, err := testHelper.RunCommandInContainer(testContext, containerName, command)
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

func deleteBackupsRetainAfterTimeViaMongodb(retainCount int, timestampId string, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	return testHelper.MongoPurgeBackupsAfterTime(testContext, containerName, retainCount, testContext.AuxData.Timestamps[timestampId])
}

func deleteBackupsRetainAfterBackupViaMongodb(retainCount int, afterBackupId int, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	return testHelper.MongoPurgeBackupsAfterBackupId(testContext, containerName, retainCount, afterBackupId)
}

func mongodbBackupMetadataContainsUserData(nodeName string, backupId int, data *gherkin.DocString) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	backupList, err := testHelper.GetBackups(testContext, containerName)
	if err != nil {
		return err
	}
	if len(backupList)-1-backupId < 0 {
		return fmt.Errorf("cannot get %dth backup - there are only %d", backupId, len(backupList))
	}
	backup := backupList[len(backupList)-1-backupId]
	path := fmt.Sprintf("/export/dbaas/mongodb-backup/test_uuid/test_mongodb/basebackups_005/%s_backup_stop_sentinel.json", backup)
	cmd := []string{"cat", path}
	jsonString, _ := testHelper.RunCommandInContainer(testContext, fmt.Sprintf("minio01.test_net_%s", testContext.Env["TEST_ID"]), cmd)
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

func testMongodbConnect(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	const timeoutInterval = 100 * time.Millisecond
	for i := 0; i < 25; i++ {
		connection, _ := testHelper.EnvDBConnect(testContext, containerName)
		err := connection.Database(containerName).Client().Ping(testContext.Context, nil)
		if err == nil {
			return nil
		}
		time.Sleep(timeoutInterval)
	}
	return fmt.Errorf("cannot connect to %s", containerName)
}

func configureS3OnMinio(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	container, err := testHelper.GetDockerContainer(testContext, containerName)
	if err != nil {
		return err
	}
	err = testHelper.ConfigureS3(testContext, container)
	if err != nil {
		return err
	}
	return nil
}

func replsetinitiateOnMongodb(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	err := testHelper.StepEnsureRsInitialized(testContext, containerName)
	if err != nil {
		return err
	}
	time.Sleep(3 * time.Second)
	return nil
}

func testMongodbPrimaryRole(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	connection, err := testHelper.AdminConnect(testContext, containerName)
	if err != nil {
		return err
	}
	smth := connection.Ping(testContext.Context, readpref.Primary())
	return smth
}

func authenticateOnMongodb(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	creds := testHelper.AdminCreds(testContext.Env)
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
		testContext.Env["MONGO_ADMIN_DB_NAME"]}
	response, err := testHelper.RunCommandInContainer(testContext, containerName, command)
	if err != nil {
		return err
	}
	if strings.Contains(response, "command createUser requires authentication") {
		command = append(command, "-u", creds.Username, "-p", creds.Password)
		response, err = testHelper.RunCommandInContainer(testContext, containerName, command)
		if err != nil {
			return err
		}
	}
	if !strings.Contains(response, "Successfully added user") &&
		!strings.Contains(response, "not authorized on admin") &&
		!strings.Contains(response, "already exists") {
		return fmt.Errorf("can not initialize auth: %s", response)
	}
	newRoleCmd := []string{"mongo", "mongodb://admin:password@127.0.0.1:27018/admin", "--eval",
		`db.createRole( {
    		role: "interalUseOnlyOplogRestore",
  			privileges: [
      			{ resource: { anyResource: true }, actions: [ "anyAction" ] }
    		],
    		roles: []
   		})`,
	}

	roleResp, err := testHelper.RunCommandInContainer(testContext, containerName, newRoleCmd)
	if err != nil {
		return err
	}

	if !(strings.Contains(roleResp, `"role" : "interalUseOnlyOplogRestore",`) ||
		strings.Contains(roleResp, `Role "interalUseOnlyOplogRestore@admin" already exists`)) {
		return fmt.Errorf("can not create role for auth: %s", roleResp)
	}

	updRoleCmd := []string{"mongo", "mongodb://admin:password@127.0.0.1:27018/admin", "--eval",
		`db.grantRolesToUser(
			"admin",
    		["interalUseOnlyOplogRestore"]
   		)`,
	}

	updRoleResp, err := testHelper.RunCommandInContainer(testContext, containerName, updRoleCmd)
	if err != nil {
		return err
	}

	if !strings.Contains(updRoleResp, ` `) {
		return fmt.Errorf("can not create role for auth: %s", updRoleResp)
	}

	return nil
}

func fillMongodbWithTestData(nodeName string, testId int) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	testName := fmt.Sprintf("test%02d", testId)
	conn, err := testHelper.AdminConnect(testContext, containerName)
	if err != nil {
		return err
	}
	data := testHelper.FillWithData(testContext.Context, conn, testName)
	testContext.TestData["test"+string(testId)] = data
	return nil
}

func createMongodbBackup(nodeName string) error {
	var cmdArgs = ""
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	currentBackupId, err := testHelper.MakeBackup(testContext, containerName, cmdArgs, []string{})
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

func createMongodbBackupWithUserData(nodeName string, data *gherkin.DocString) error {
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
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	currentBackupId, err := testHelper.MakeBackup(testContext, containerName, cmdArgs, envs)
	if err != nil {
		return err
	}
	testContext.SafeStorage.CreatedBackupNames = append(testContext.SafeStorage.CreatedBackupNames, currentBackupId)
	return nil
}

func testBackupEntriesOfMongodb(backupCount int, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	backupNames, err := testHelper.GetBackups(testContext, containerName)
	if err != nil {
		return err
	}
	if len(backupNames) != backupCount {
		return fmt.Errorf("expected %d number of backups, but found %d", backupCount, len(backupNames))
	}
	return nil
}

func putEmptyBackupViaMinio(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	backupName := "20010203T040506"
	bucketName := testContext.Env["S3_BUCKET"]
	backupRootDir := testContext.Env["WALG_S3_PREFIX"]
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

func deleteBackupsRetainViaMongodb(retainCount int, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	return testHelper.MongoPurgeBackups(testContext, containerName, retainCount)
}

func testEmptyBackupsViaMinio(nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	bucketName := testContext.Env["S3_BUCKET"]
	backupRootDir := testContext.Env["WALG_S3_PREFIX"]
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

func deleteBackupViaMongodb(backupId int, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	return testHelper.DeleteBackup(testContext, containerName, backupId)
}

func restoreBackupToMongodb(backupId int, nodeName string) error {
	containerName := fmt.Sprintf("%s.test_net_%s", nodeName, testContext.Env["TEST_ID"])
	return testHelper.RestoreBackupById(testContext, containerName, backupId)
}

func testEqualMongodbDataAtMongodbs(nodeName1, nodeName2 string) error {
	containerName1 := fmt.Sprintf("%s.test_net_%s", nodeName1, testContext.Env["TEST_ID"])
	containerName2 := fmt.Sprintf("%s.test_net_%s", nodeName2, testContext.Env["TEST_ID"])

	connection1, err := testHelper.AdminConnect(testContext, containerName1)
	if err != nil {
		return err
	}
	connection2, err := testHelper.AdminConnect(testContext, containerName2)
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

var cleanEnv bool

func init() {
	flag.BoolVar(&cleanEnv, "env.clean", false, "shutdown and delete test environment")
	godog.BindFlags("godog.", flag.CommandLine, &opt)
}

func TestMain(m *testing.M) {
	flag.Parse()
	opt.Paths = flag.Args()

	testContext = &testHelper.TestContextType{}
	if err := SetupTestContext(testContext); err != nil {
		log.Fatalln(err)
	}

	if cleanEnv {
		env, err := ReadEnv(testConf.Env["ENV_FILE"])
		if err != nil {
			log.Fatalln(err)
		}
		testContext.Env = env

		if err := ShutdownEnv(testContext); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}

	status := godog.RunWithOptions("godogs", func(s *godog.Suite) {
		FeatureContext(s)
	}, opt)

	if st := m.Run(); st > status {
		status = st
	}

	if testUtils.ParseEnvLines(os.Environ())["DEBUG"] == "" {
		if err := ShutdownEnv(testContext); err != nil {
			log.Fatal(err)
		}
	}

	os.Exit(status)
}
