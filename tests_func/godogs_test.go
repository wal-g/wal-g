package functest

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/gherkin"
	. "github.com/wal-g/wal-g/tests_func/helpers"
	. "github.com/wal-g/wal-g/tests_func/utils"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

var testContext = &TestContextType{}

func FeatureContext(s *godog.Suite) {

	testContext.TestData = make(map[string]map[string]map[string][]DatabaseRecord)

	s.BeforeFeature(func(feature *gherkin.Feature) {
		SetupStaging(testContext)
		BuildBase(testContext)
		Start(testContext)
	})

	s.AfterFeature(func(feature *gherkin.Feature) {
		ShutdownContainers(testContext)
		ShutdownNetwork(testContext)
	})

	s.BeforeStep(func(s *gherkin.Step){

	})

	s.AfterStep(func (s *gherkin.Step, err error){
	})

	s.Step(`^a working mongodb on mongodb(\d+)$`, testMongodbConnect)
	s.Step(`^a configured s3 on minio(\d+)$`, configureS3OnMinio)
	s.Step(`^mongodb replset initialized on mongodb(\d+)$`, replsetinitiateOnMongodb)
	s.Step(`^mongodb role is primary on mongodb(\d+)$`, testMongodbPrimaryRole)
	s.Step(`^mongodb auth initialized on mongodb(\d+)$`, authenticateOnMongodb)
	s.Step(`^mongodb(\d+) has test mongodb data test(\d+)$`, fillMongodbWithTestData)
	s.Step(`^we create mongodb(\d+) backup$`, createMongodbBackup)
	s.Step(`^we got (\d+) backup entries of mongodb(\d+)$`, testBackupEntriesOfMongodb)
	s.Step(`^we put empty backup via minio(\d+)$`, putEmptyBackupViaMinio)
	s.Step(`^we delete backups retain (\d+) via mongodb(\d+)$`, deleteBackupsRetainViaMongodb)
	s.Step(`^we check if empty backups were purged via minio(\d+)$`, testEmptyBackupsViaMinio)

	s.Step(`^we delete #(\d+) backup via mongodb(\d+)$`, deleteBackupViaMongodb)
	s.Step(`^we restore #(\d+) backup to mongodb(\d+)$`, restoreBackupToMongodb)
	s.Step(`^we got same mongodb data at mongodb(\d+) mongodb(\d+)$`, testEqualMongodbDataAtMongodbs)

}

func testMongodbConnect(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	for i := 0; i < 25; i++ {
		connection := EnvDBConnect(testContext, nodeName)
		err := connection.Database(nodeName).Client().Ping(context.Background(), nil)
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("cannot connect to %s", nodeName)
}

func configureS3OnMinio(arg1 int) error {
	nodeName := fmt.Sprintf("minio%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	container := GetDockerContainer(testContext, nodeName)
	ConfigureS3(testContext, container)
	return nil
}

func replsetinitiateOnMongodb(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	StepEnsureRsInitialized(testContext, nodeName)
	return nil
}

func testMongodbPrimaryRole(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	connection := EnvDBConnectWithCreds(testContext, nodeName, creds)
	smth := connection.Ping(context.Background(), readpref.Primary())
	return smth
}

func authenticateOnMongodb(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
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
		testContext.Configuration.Projects["mongodb"].Users["admin"].Dbname}
	response := RunCommandInContainer(testContext, nodeName, command)
	if !strings.Contains(response, "Successfully added user") &&
		!strings.Contains(response, "not authorized on admin"){
		return fmt.Errorf("can not initialize auth: %s", response)
	}
	return nil
}

func fillMongodbWithTestData(arg1, arg2 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	testName := fmt.Sprintf("test%02d", arg2)
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	conn := EnvDBConnectWithCreds(testContext, nodeName, creds)
	data := FillWithData(conn, testName)
	testContext.TestData["test" + string(arg2)] = data
	return nil
}

func createMongodbBackup(arg1 int) error {
	var cmdArgs = ""
	containerName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	currentBackupId := MakeBackup(testContext, containerName, cmdArgs, creds)
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

func createMongodbBackupWithContent(arg1 int, arg2 *gherkin.DocString) error {
	var cmdArgs = ""
	if arg2 != nil {
		content := getMakeBackupContentFromDocString(arg2)
		fmt.Println(content)
		args := []string{}
		if labels, ok := content["labels"]; ok {
			for key, value := range labels {
				args = append(args, fmt.Sprintf("--label %s=%s", key, value))
			}
		}
		if name, ok := content["name"]; ok {
			args = append(args, fmt.Sprintf("--name %s", name))
		}
	}
	containerName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	currentBackupId := MakeBackup(testContext, containerName, cmdArgs, creds)
	testContext.SafeStorage.CreatedBackupNames = append(testContext.SafeStorage.CreatedBackupNames, currentBackupId)
	return nil
}

func testBackupEntriesOfMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	backupNames := GetBackups(testContext, containerName)
	if len(backupNames) != arg1 {
		return fmt.Errorf("expected %d number of backups, but found %d", arg1, len(backupNames))
	}
	return nil
}

func putEmptyBackupViaMinio(arg1 int) error {
	containerName := fmt.Sprintf("minio%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	backupName := "20010203T040506"
	bucketName := testContext.Configuration.DynamicConfiguration.S3.Bucket
	backupRootDir := testContext.Configuration.DynamicConfiguration.Walg.Path
	backupDir := "/export/" + bucketName + "/" + backupRootDir + "/" + backupName
	backupDumpPath := filepath.Join(backupDir, "mongodump.archive")
	testContext.SafeStorage.NometaBackupNames = append(testContext.SafeStorage.NometaBackupNames, backupName)
	_ = RunCommandInContainer(testContext, containerName, []string{"mkdir", "-p", backupDir})
	_ = RunCommandInContainer(testContext, containerName, []string{"touch", backupDumpPath})
	return nil
}

func deleteBackupsRetainViaMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	MongoPurgeBackups(testContext, containerName, arg1)
	return nil
}

func testEmptyBackupsViaMinio(arg1 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	bucketName := testContext.Configuration.DynamicConfiguration.S3.Bucket
	backupRootDir := testContext.Configuration.DynamicConfiguration.Walg.Path
	backupNames := testContext.SafeStorage.NometaBackupNames
	for _, backupName := range backupNames {
		backupDir := filepath.Join("/export", bucketName, backupRootDir, backupName)
		_ = RunCommandInContainer(testContext, containerName, []string{"ls", backupDir})
	}
	return nil
}

func deleteBackupViaMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	DeleteBackup(testContext, containerName, arg1)
	return nil
}

func restoreBackupToMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	RestoreBackupById(testContext, containerName, arg1)
	return nil
}

func testEqualMongodbDataAtMongodbs(arg1, arg2 int) error {
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	containerName1 := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	containerName2 := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")

	connection1 := EnvDBConnectWithCreds(testContext, containerName1, creds)
	connection2 := EnvDBConnectWithCreds(testContext, containerName2, creds)

	var userData [][]UserData
	rowsData1 := GetAllUserData(connection1)
	rowsData2 := GetAllUserData(connection2)

	userData = append(userData, rowsData1)
	userData = append(userData, rowsData2)

	if !reflect.DeepEqual(rowsData1, rowsData2) {
		return fmt.Errorf("expected the same data in %s and %s", containerName1, containerName2)
	}
	return nil
}
