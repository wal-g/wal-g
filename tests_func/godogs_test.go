package main

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/godog"
	"github.com/DATA-DOG/godog/gherkin"
	"github.com/docker/docker/api/types"
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

	s.Step(`^a working mongodb on mongodb(\d+)$`, aWorkingMongodbOnMongodb)
	s.Step(`^a configured s3 on minio(\d+)$`, aConfiguredSOnMinio)
	s.Step(`^mongodb replset initialized on mongodb(\d+)$`, mongodbReplsetInitializedOnMongodb)
	s.Step(`^mongodb role is primary on mongodb(\d+)$`, mongodbRoleIsPrimaryOnMongodb)
	s.Step(`^mongodb auth initialized on mongodb(\d+)$`, mongodbAuthInitializedOnMongodb)
	s.Step(`^a trusted gpg keys on mongodb(\d+)$`, aTrustedGpgKeysOnMongodb)
	s.Step(`^mongodb(\d+) has test mongodb data test(\d+)$`, mongodbHasTestMongodbDataTest)
	s.Step(`^we create mongodb(\d+) backup$`, weCreateMongodbBackup)
	s.Step(`^we got (\d+) backup entries of mongodb(\d+)$`, weGotBackupEntriesOfMongodb)
	s.Step(`^we put empty backup via minio(\d+)$`, wePutEmptyBackupViaMinio)
	s.Step(`^we delete backups retain (\d+) via mongodb(\d+)$`, weDeleteBackupsRetainViaMongodb)
	s.Step(`^we check if empty backups were purged via minio(\d+)$`, weCheckIfEmptyBackupsWerePurgedViaMinio)

	s.Step(`^we delete #(\d+) backup via mongodb(\d+)$`, weDeleteBackupViaMongodb)
	s.Step(`^we restore #(\d+) backup to mongodb(\d+)$`, weRestoreBackupToMongodb)
	s.Step(`^we got same mongodb data at mongodb(\d+) mongodb(\d+)$`, weGotSameMongodbDataAtMongodbMongodb)

}

func aWorkingMongodbOnMongodb(arg1 int) error {
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

func aConfiguredSOnMinio(arg1 int) error {
	nodeName := fmt.Sprintf("minio%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	container := GetDockerContainer(testContext, nodeName)
	ConfigureS3(testContext, container)
	return nil
}

func mongodbReplsetInitializedOnMongodb(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	StepEnsureRsInitialized(testContext, nodeName)
	return nil
}

func mongodbRoleIsPrimaryOnMongodb(arg1 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	connection := EnvDBConnectWithCreds(testContext, nodeName, creds)
	smth := connection.Ping(context.Background(), readpref.Primary())
	return smth
}

func mongodbAuthInitializedOnMongodb(arg1 int) error {
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

func aTrustedGpgKeysOnMongodb(arg1 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	command := []string{"gpg", "--list-keys", "--list-options", "show-uid-validity"}
	response := RunCommandInContainerWithOptions(testContext, containerName, command, types.ExecConfig{User: testContext.Configuration.DynamicConfiguration.gpg.user})

	if strings.Contains(response, "[ultimate] test_cluster") {
		return nil
	}

	homedir := testContext.Configuration.DynamicConfiguration.gpg.homedir
	command = []string{"gpg", "--homedir", homedir, "--no-tty", "--import", "/config/gpg-key.armor"}
	response = RunCommandInContainerWithOptions(testContext, containerName, command, types.ExecConfig{User: testContext.Configuration.DynamicConfiguration.gpg.user, Tty: true})

	if !strings.Contains(response,"secret keys imported: 1") {
		panic(fmt.Errorf("can not import keys: %s", response))
	}

	longcmd := fmt.Sprintf(`for key in $(gpg --no-tty --homedir %s -k | grep ^pub |
cut -d'/' -f2 | awk '{print $1};' 2>/dev/null); do
	printf "trust\n5\ny\nquit" | \
	gpg --homedir %s --debug --no-tty --command-fd 0 \
		--edit-key ${key};
done`, homedir, homedir)

	command = []string{"bash", "-c", longcmd}
	response = RunCommandInContainerWithOptions(testContext, containerName, command, types.ExecConfig{User: testContext.Configuration.DynamicConfiguration.gpg.user, Tty: true})

	command = []string{"gpg", "--list-keys", "--list-options", "show-uid-validity"}
	response = RunCommandInContainerWithOptions(testContext, containerName, command, types.ExecConfig{User: testContext.Configuration.DynamicConfiguration.gpg.user})

	if !strings.Contains(response, "[ultimate] test_cluster") {
		return fmt.Errorf("can not trust keys: %s", response)
	}
	return nil
}

func mongodbHasTestMongodbDataTest(arg1, arg2 int) error {
	nodeName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	testName := fmt.Sprintf("test%02d", arg2)
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	conn := EnvDBConnectWithCreds(testContext, nodeName, creds)
	data := FillWithData(conn, testName)
	testContext.TestData["test" + string(arg2)] = data
	return nil
}

func weCreateMongodbBackup(arg1 int) error {
	var cmdArgs = ""
	containerName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	creds := testContext.Configuration.Projects["mongodb"].Users["admin"]
	currentBackupId := MakeBackup(testContext, containerName, cmdArgs, creds)
	testContext.SafeStorage.CreatedBackupNames = append(testContext.SafeStorage.CreatedBackupNames, currentBackupId)
	return nil
}

func weGotBackupEntriesOfMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	backupNames := GetBackups(testContext, containerName)
	if len(backupNames) != arg1 {
		return fmt.Errorf("expected %d number of backups, but found %d", arg1, len(backupNames))
	}
	return nil
}

func wePutEmptyBackupViaMinio(arg1 int) error {
	containerName := fmt.Sprintf("minio%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	backupName := "20010203T040506"
	bucketName := testContext.Configuration.DynamicConfiguration.s3.bucket
	backupRootDir := testContext.Configuration.DynamicConfiguration.walg.path
	backupDir := "/export/" + bucketName + "/" + backupRootDir + "/" + backupName
	backupDumpPath := filepath.Join(backupDir, "mongodump.archive")
	testContext.SafeStorage.NometaBackupNames = append(testContext.SafeStorage.NometaBackupNames, backupName)
	_ = RunCommandInContainer(testContext, containerName, []string{"mkdir", "-p", backupDir})
	_ = RunCommandInContainer(testContext, containerName, []string{"touch", backupDumpPath})
	return nil
}

func weDeleteBackupsRetainViaMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	MongoPurgeBackups(testContext, containerName, arg1)
	return nil
}

func weCheckIfEmptyBackupsWerePurgedViaMinio(arg1 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg1) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	bucketName := testContext.Configuration.DynamicConfiguration.s3.bucket
	backupRootDir := testContext.Configuration.DynamicConfiguration.walg.path
	backupNames := testContext.SafeStorage.NometaBackupNames
	for _, backupName := range backupNames {
		backupDir := filepath.Join("/export", bucketName, backupRootDir, backupName)
		_ = RunCommandInContainer(testContext, containerName, []string{"ls", backupDir})
	}
	return nil
}

func weDeleteBackupViaMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	DeleteBackup(testContext, containerName, arg1)
	return nil
}

func weRestoreBackupToMongodb(arg1, arg2 int) error {
	containerName := fmt.Sprintf("mongodb%02d", arg2) + ".test_net_" + GetVarFromEnvList(testContext.Env, "TEST_ID")
	restoreBackupById(testContext, containerName, arg1)
	return nil
}

func weGotSameMongodbDataAtMongodbMongodb(arg1, arg2 int) error {
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

