package functest

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/gherkin"
	testHelper "github.com/wal-g/wal-g/tests_func/helpers"
	testUtils "github.com/wal-g/wal-g/tests_func/utils"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

var testContext = &testHelper.TestContextType{}

func FeatureContext(s *godog.Suite) {

	testContext.TestData = make(map[string]map[string]map[string][]testHelper.DatabaseRecord)
	testContext.Context = context.Background()

	s.BeforeFeature(func(feature *gherkin.Feature) {
		err := SetupStaging(testContext)
		if err != nil {
			panic(err)
		}
		err = BuildBase(testContext)
		if err != nil {
			panic(err)
		}
		err = Start(testContext)
		if err != nil {
			panic(err)
		}
	})

	s.AfterFeature(func(feature *gherkin.Feature) {
		err := testHelper.ShutdownContainers(testContext)
		if err != nil {
			panic(err)
		}
		err = testHelper.ShutdownNetwork(testContext)
		if err != nil {
			panic(err)
		}
		err = os.RemoveAll("./staging/images/")
		if err != nil {
			panic(err)
		}
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

}

func mongodbBackupMetadataContainsUserData(arg1, arg2 int, arg3 *gherkin.DocString) error {
	return nil
}


func testMongodbConnect(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	for i := 0; i < 25; i++ {
		connection, _ := testHelper.EnvDBConnect(testContext, nodeName)
		err := connection.Database(nodeName).Client().Ping(testContext.Context, nil)
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("cannot connect to %s", nodeName)
}

func configureS3OnMinio(arg1 int) error {
	nodeName := fmt.Sprintf("minio%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
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

func replsetinitiateOnMongodb(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	err := testHelper.StepEnsureRsInitialized(testContext, nodeName)
	if err != nil {
		return err
	}
	return nil
}

func testMongodbPrimaryRole(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
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

func authenticateOnMongodb(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
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
	if !strings.Contains(response, "Successfully added user") &&
		!strings.Contains(response, "not authorized on admin") {
		return fmt.Errorf("can not initialize auth: %s", response)
	}
	return nil
}

func fillMongodbWithTestData(arg1, arg2 int) error {
	nodeName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	testName := fmt.Sprintf("test%02d", arg2)
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
	testContext.TestData["test"+string(arg2)] = data
	return nil
}

func createMongodbBackup(arg1 int) error {
	var cmdArgs = ""
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
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
			values := strings.Split(line, " ")
			innerKey := strings.Trim(values[0], ": ")
			value := strings.Trim(values[1], ": ")
			res[outerKey][innerKey] = value
		}
	}
	return res
}

func createMongodbBackupWithUserData(arg1 int, arg2 *gherkin.DocString) error {
	var cmdArgs = ""
	var envs []string
	if arg2 != nil {
		content := getMakeBackupContentFromDocString(arg2)
		var args []string
		if labels, ok := content["labels"]; ok {
			for key, value := range labels {
				args = append(args, fmt.Sprintf("\"%s\": \"%s\"", key, value))
			}
		}
		envs = append(envs, "WALG_SENTINEL_USER_DATA='{" + strings.Join(args, ", ") + "}'")
	}
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
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

func testBackupEntriesOfMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", arg2, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	backupNames, err := testHelper.GetBackups(testContext, containerName)
	if err != nil {
		return err
	}
	if len(backupNames) != arg1 {
		return fmt.Errorf("expected %d number of backups, but found %d", arg1, len(backupNames))
	}
	return nil
}

func putEmptyBackupViaMinio(arg1 int) error {
	containerName := fmt.Sprintf("minio%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
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

func deleteBackupsRetainViaMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", arg2, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.MongoPurgeBackups(testContext, containerName, arg1)
}

func testEmptyBackupsViaMinio(arg1 int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", arg1, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
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

func deleteBackupViaMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", arg2, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.DeleteBackup(testContext, containerName, arg1)
}

func restoreBackupToMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d.test_net_%s", arg2, testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID"))
	return testHelper.RestoreBackupById(testContext, containerName, arg1)
}

func testEqualMongodbDataAtMongodbs(arg1, arg2 int) error {
	creds := testHelper.UserConfiguration{
		Username: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_USERNAME"),
		Password: testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_PASSWORD"),
		Dbname:   testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_DB_NAME"),
		Roles:    strings.Split(testUtils.GetVarFromEnvList(testContext.Env, "MONGO_ADMIN_ROLES"), " "),
	}
	containerName1 := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")
	containerName2 := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + testUtils.GetVarFromEnvList(testContext.Env, "TEST_ID")

	connection1, err := testHelper.EnvDBConnectWithCreds(testContext, containerName1, creds)
	if err != nil {
		return err
	}
	connection2, err := testHelper.EnvDBConnectWithCreds(testContext, containerName2, creds)
	if err != nil {
		return err
	}

	var userData [][]testHelper.UserData
	rowsData1 := testHelper.GetAllUserData(testContext.Context, connection1)
	rowsData2 := testHelper.GetAllUserData(testContext.Context, connection2)

	userData = append(userData, rowsData1)
	userData = append(userData, rowsData2)

	if !reflect.DeepEqual(rowsData1, rowsData2) {
		return fmt.Errorf("expected the same data in %s and %s", containerName1, containerName2)
	}
	return nil
}
