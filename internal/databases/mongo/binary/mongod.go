package binary

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const adminDB = "admin"

const cursorCreateRetries = 10
const mongoConnectRetries = 3

type MongodService struct {
	Context     context.Context
	MongoClient *mongo.Client
}

func CreateMongodService(ctx context.Context, appName, mongodbURI string, timeout time.Duration) (*MongodService, error) {
	var repeatOptions backoff.BackOff
	repeatOptions = backoff.NewExponentialBackOff()
	repeatOptions = backoff.WithMaxRetries(repeatOptions, mongoConnectRetries)
	repeatOptions = backoff.WithContext(repeatOptions, ctx)

	var mongoClient *mongo.Client
	var err error
	err = backoff.RetryNotify(
		func() error {
			mongoClient, err = mongo.Connect(ctx,
				options.Client().ApplyURI(mongodbURI).
					SetServerSelectionTimeout(timeout).
					SetConnectTimeout(timeout).
					SetSocketTimeout(time.Minute).
					SetAppName(appName).
					SetDirect(true).
					SetRetryReads(false))
			if err != nil {
				return errors.Wrap(err, "unable to connect to mongod")
			}
			err = mongoClient.Ping(ctx, nil)
			if err != nil {
				return errors.Wrap(err, "ping to mongod is failed")
			}
			return nil
		},
		repeatOptions,
		func(err error, duration time.Duration) {
			tracelog.InfoLogger.Printf("Unable to connect due '%+v', next retry: %v", err, duration)
		},
	)
	if err != nil {
		return nil, err
	}

	return &MongodService{
		Context:     ctx,
		MongoClient: mongoClient,
	}, nil
}

func (mongodService *MongodService) MongodVersion() (string, error) {
	versionHolder := struct {
		Version string `bson:"version"`
	}{}
	err := mongodService.MongoClient.Database(adminDB).RunCommand(
		mongodService.Context,
		bson.M{"buildInfo": 1},
	).Decode(&versionHolder)

	return versionHolder.Version, err
}

type MongodConfig struct {
	Net struct {
		BindIP string `bson:"bindIp" json:"bindIp"`
		Port   int    `bson:"port" json:"port"`
	} `bson:"net" json:"net"`
	Storage struct {
		DBPath string `bson:"dbPath" json:"dbPath"`
	} `bson:"storage" json:"storage"`
}

func (mongodService *MongodService) MongodConfig() (*MongodConfig, error) {
	getCmdLineOpts := struct {
		Parsed MongodConfig `bson:"parsed" json:"parsed"`
	}{}
	err := mongodService.MongoClient.Database(adminDB).RunCommand(
		mongodService.Context,
		bson.M{"getCmdLineOpts": 1},
	).Decode(&getCmdLineOpts)

	return &getCmdLineOpts.Parsed, err
}

func (mongodService *MongodService) GetReplSetName() (string, error) {
	replSetNameHolder := struct {
		ReplSetName string `bson:"setName"`
	}{}
	err := mongodService.MongoClient.Database(adminDB).RunCommand(
		mongodService.Context,
		bson.M{"isMaster": 1},
	).Decode(&replSetNameHolder)

	return replSetNameHolder.ReplSetName, err
}

func (mongodService *MongodService) GetBackupCursor() (cursor *mongo.Cursor, err error) {
	for i := 0; i < cursorCreateRetries; i++ {
		cursor, err = mongodService.MongoClient.Database(adminDB).Aggregate(mongodService.Context, mongo.Pipeline{
			{{Key: "$backupCursor", Value: bson.D{}}},
		})
		if err == nil {
			break // success!
		}
		if !backupCursorErrorIsRetried(err) {
			return nil, err
		}
		if i < cursorCreateRetries {
			minutes := time.Duration(i + 1)
			tracelog.WarningLogger.Printf("%v. Sleep %d minutes and retry", err.Error(), minutes)
			time.Sleep(time.Minute * minutes)
		}
	}

	return cursor, err
}

func backupCursorErrorIsRetried(err error) bool {
	return strings.Contains(err.Error(), "(Location50915)") ||
		strings.Contains(err.Error(), "(BackupCursorOpenConflictWithCheckpoint)") // mongodb take checkpoint
}

func (mongodService *MongodService) GetBackupCursorExtended(backupCursorMeta *BackupCursorMeta) (*mongo.Cursor, error) {
	return mongodService.MongoClient.Database(adminDB).Aggregate(mongodService.Context, mongo.Pipeline{
		{{
			Key: "$backupCursorExtend", Value: bson.D{
				{Key: "backupId", Value: backupCursorMeta.ID},
				{Key: "timestamp", Value: backupCursorMeta.OplogEnd.TS},
			},
		}},
	})
}

func (mongodService *MongodService) FixReplset(rsConfig RsConfig) error {
	ctx := mongodService.Context
	localDatabase := mongodService.MongoClient.Database("local")

	if err := replaceData(ctx, localDatabase.Collection("replset.election"), true, nil); err != nil {
		return errors.Wrap(err, "unable to fix data in local.replset.election")
	}

	if rsConfig.Empty() {
		if err := replaceData(ctx, localDatabase.Collection("system.replset"), false, nil); err != nil {
			return errors.Wrap(err, "unable to fix data in local.system.replset")
		}
	} else {
		if err := updateRsConfig(ctx, localDatabase, rsConfig); err != nil {
			return err
		}
	}

	return nil
}

func (mongodService *MongodService) FixShardIdentity(shConfig ShConfig) error {
	ctx := mongodService.Context
	adminDatabase := mongodService.MongoClient.Database(adminDB)

	if shConfig.Empty() {
		_, err := adminDatabase.Collection("system.version").DeleteOne(ctx, bson.D{
			{Key: "_id", Value: "shardIdentity"},
		})
		if err != nil {
			return errors.Wrap(err, "unable to fix data in admin.system.version")
		}
	} else {
		val := adminDatabase.Collection("system.version").FindOne(ctx, bson.D{{Key: "_id", Value: "shardIdentity"}})
		if val.Err() != nil {
			tracelog.WarningLogger.Printf("Unable to find system.version in admin database. Skipping this step, assuming oplog replay will fix this")
		} else {
			var systemShConfig bson.M
			err := val.Decode(&systemShConfig)
			if err != nil {
				return errors.Wrap(err, "couldn't decode shard config")
			}

			systemShConfig["shardName"] = shConfig.ShardName
			systemShConfig["configsvrConnectionString"] = shConfig.MongoCfgConnectionString

			_, err = adminDatabase.Collection("system.version").
				UpdateOne(ctx,
					bson.D{{Key: "_id", Value: "shardIdentity"}},
					bson.D{{Key: "$set", Value: systemShConfig}})
			if err != nil {
				return errors.Wrap(err, "unable to update shardIdentity in system.version")
			}
			tracelog.InfoLogger.Printf("Successfully fixed admin.system.version document with proper shardIdentity")
		}
	}
	return nil
}

func (mongodService *MongodService) FixMongoCfg(mongocfgConfig MongoCfgConfig) error {
	if mongocfgConfig.Empty() {
		return nil
	}

	ctx := mongodService.Context
	configDatabase := mongodService.MongoClient.Database("config")

	_, err := configDatabase.Collection("mongos").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "failed to drop config.mongos collection")
	}
	_, err = configDatabase.Collection("lockpings").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "failed to drop config.lockpings collection")
	}
	_, err = configDatabase.Collection("shards").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "failed to drop config.shards collection")
	}
	for shardName, connStr := range mongocfgConfig.Shards {
		_, err := configDatabase.Collection("shards").InsertOne(ctx, bson.D{{Key: "_id", Value: shardName}, {Key: "host", Value: connStr}})
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("unable to insert shard info for name '%s'", shardName))
		}
	}
	tracelog.InfoLogger.Printf("Successfully updated config.shards collection")

	return nil
}

func (mongodService *MongodService) CleanCacheAndSessions(shConfig ShConfig) error {
	ctx := mongodService.Context
	configDatabase := mongodService.MongoClient.Database("config")

	colls, err := configDatabase.ListCollectionNames(ctx, bson.D{{Key: "name", Value: bson.M{"$regex": `^cache\.`}}})
	if err != nil {
		return errors.Wrap(err, "failed to list config.cache.* collections")
	}
	for _, coll := range colls {
		_, err := configDatabase.Collection(coll).DeleteMany(ctx, bson.D{})
		if err != nil {
			return errors.Wrapf(err, "failed to drop %s collection", coll)
		}
	}

	const retry = 5
	for i := 0; i < retry; i++ {
		_, err = configDatabase.Collection("system.sessions").DeleteMany(ctx, bson.D{})
		if err == nil || !strings.Contains(err.Error(), "(BackgroundOperationInProgressForNamespace)") {
			break
		}
		tracelog.DebugLogger.Printf("drop config.system.sessions: BackgroundOperationInProgressForNamespace, retrying")
		time.Sleep(time.Second * time.Duration(i+1))
	}
	if err != nil {
		return errors.Wrap(err, "drop config.system.sessions")
	}
	return nil
}

func updateRsConfig(ctx context.Context, localDatabase *mongo.Database, rsConfig RsConfig) error {
	var systemRsConfig bson.M
	err := localDatabase.Collection("system.replset").FindOne(ctx, bson.D{}).Decode(&systemRsConfig)
	if err != nil {
		return errors.Wrap(err, "unable to read rs config from system.replset")
	}

	systemRsConfig["members"] = makeBsonRsMembers(rsConfig)

	if systemRsConfig["_id"] != rsConfig.RsName {
		systemRsConfig["_id"] = rsConfig.RsName
		_, err = localDatabase.Collection("system.replset").InsertOne(ctx, systemRsConfig)
		if err != nil {
			return errors.Wrap(err, "unable to insert updated rs config to system.replset")
		}
		deleteResult, err := localDatabase.Collection("system.replset").
			DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$ne", Value: rsConfig.RsName}}}})
		if err != nil {
			return errors.Wrap(err, "unable to delete other documents to system.replset")
		}
		tracelog.DebugLogger.Printf("Removed %d documents from system.replset", deleteResult.DeletedCount)
	} else {
		updateResult, err := localDatabase.Collection("system.replset").
			UpdateMany(ctx,
				bson.D{{Key: "_id", Value: systemRsConfig["_id"]}},
				bson.D{{Key: "$set", Value: systemRsConfig}})
		if err != nil {
			return errors.Wrap(err, "unable to update rs config in system.replset")
		}
		if updateResult.MatchedCount != 1 {
			return errors.Errorf("MatchedCount = %v during update rs config in system.replset",
				updateResult.MatchedCount)
		}
		tracelog.InfoLogger.Printf("Updated %d documents in system.replset", updateResult.MatchedCount)
	}

	return nil
}

func replaceData(ctx context.Context, collection *mongo.Collection, drop bool, insertData bson.M) error {
	findCursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("unable to find all from %v", collection.Name()))
	}
	data := bson.D{}
	for findCursor.Next(ctx) {
		err := findCursor.Decode(&data)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("unable to decode item from %v", collection.Name()))
		}
		bytes, err := json.Marshal(data)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("unable to marchal data from %v", collection.Name()))
		}
		tracelog.InfoLogger.Printf("[%v] %v", collection.Name(), string(bytes))
	}

	if drop {
		err = collection.Drop(ctx)
	} else {
		_, err = collection.DeleteMany(ctx, bson.D{})
	}
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("unable to drop data from %v", collection.Name()))
	}

	if insertData != nil {
		_, err = collection.InsertOne(ctx, insertData)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("unable to insert data to %v", collection.Name()))
		}
	}

	return nil
}

func makeBsonRsMembers(rsConfig RsConfig) bson.A {
	bsonMembers := bson.A{}

	for i := 0; i != len(rsConfig.RsMembers); i++ {
		bsonMembers = append(bsonMembers, bson.M{"_id": rsConfig.RsMemberIDs[i], "host": rsConfig.RsMembers[i]})
	}

	return bsonMembers
}

func (mongodService *MongodService) Shutdown() error {
	err := mongodService.MongoClient.Database(adminDB).RunCommand(context.Background(),
		bson.D{{Key: "shutdown", Value: 1}},
	).Err()
	if err != nil && !strings.Contains(err.Error(), "socket was unexpectedly closed") {
		return errors.Wrap(err, "unable to shutdown mongod")
	}
	return nil
}

type BackupCursorOplogTS struct {
	TS primitive.Timestamp `bson:"ts"`
	T  int64               `bson:"t"`
}

type BackupCursorMeta struct {
	ID                       primitive.Binary    `bson:"backupId"`
	DBPath                   string              `bson:"dbpath"`
	OplogStart               BackupCursorOplogTS `bson:"oplogStart"`
	OplogEnd                 BackupCursorOplogTS `bson:"oplogEnd"`
	CheckpointTS             primitive.Timestamp `bson:"checkpointTimestamp"`
	DisableIncrementalBackup bool                `bson:"disableIncrementalBackup"`
	IncrementalBackup        bool                `bson:"incrementalBackup"`
	BlockSize                int64               `bson:"blockSize"`
}

type BackupCursorFile struct {
	FileName string `bson:"filename" json:"filename"`
	FileSize int64  `bson:"fileSize" json:"fileSize"`
}

type RestoreArgs struct {
	BackupName     string
	RestoreVersion string

	SkipBackupDownload bool
	SkipChecks         bool
	SkipMongoReconfig  bool
}

type RsConfig struct {
	RsName      string
	RsMembers   []string
	RsMemberIDs []int
}

type ReplyOplogConfig struct {
	Since models.Timestamp
	Until models.Timestamp

	IgnoreErrCodes map[string][]int32

	OplogAlwaysUpsert    *bool
	OplogApplicationMode *string

	HasPitr bool
}

type ShConfig struct {
	ShardName                string
	MongoCfgConnectionString string
}

type MongoCfgConfig struct {
	Shards map[string]string
}

func NewRsConfig(rsName string, rsMembers []string, rsMemberIDs []int) RsConfig {
	if len(rsMemberIDs) == 0 {
		rsMemberIDs = make([]int, len(rsMembers))
		for i := 0; i < len(rsMembers); i++ {
			rsMemberIDs[i] = i
		}
	}
	return RsConfig{
		RsName:      rsName,
		RsMembers:   rsMembers,
		RsMemberIDs: rsMemberIDs,
	}
}

func NewShConfig(shardName string, connectionString string) ShConfig {
	return ShConfig{
		ShardName:                shardName,
		MongoCfgConnectionString: connectionString,
	}
}

func NewReplyOplogConfig(sincePitrStr string, untilPitrStr string) (roConfig ReplyOplogConfig, err error) {
	if sincePitrStr == "" || untilPitrStr == "" {
		return roConfig, err
	}
	roConfig.HasPitr = true
	roConfig.Since, err = models.TimestampFromStr(sincePitrStr)
	if err != nil {
		return roConfig, err
	}
	roConfig.Until, err = models.TimestampFromStr(untilPitrStr)
	if err != nil {
		return roConfig, err
	}
	if ignoreErrCodesStr, ok := conf.GetSetting(conf.OplogReplayIgnoreErrorCodes); ok {
		if err = json.Unmarshal([]byte(ignoreErrCodesStr), &roConfig.IgnoreErrCodes); err != nil {
			return roConfig, err
		}
	}

	oplogAlwaysUpsert, hasOplogAlwaysUpsert, err := conf.GetBoolSetting(conf.OplogReplayOplogAlwaysUpsert)
	if err != nil {
		return roConfig, err
	}
	if hasOplogAlwaysUpsert {
		roConfig.OplogAlwaysUpsert = &oplogAlwaysUpsert
	}

	if oplogApplicationMode, hasOplogApplicationMode := conf.GetSetting(
		conf.OplogReplayOplogApplicationMode); hasOplogApplicationMode {
		roConfig.OplogApplicationMode = &oplogApplicationMode
	}
	return roConfig, err
}

func NewMongoCfgConfig(shardConnectionStrings []string) (MongoCfgConfig, error) {
	res := MongoCfgConfig{
		Shards: make(map[string]string),
	}
	for _, shardConnStr := range shardConnectionStrings {
		shardName, _, found := strings.Cut(shardConnStr, "/")
		if !found {
			return res, fmt.Errorf("%s does not contain shard name separator '/'", shardConnStr)
		}
		res.Shards[shardName] = shardConnStr
	}
	return res, nil
}

func (rsConfig RsConfig) Empty() bool {
	return rsConfig.RsName == "" && len(rsConfig.RsMembers) == 0
}

func (shConfig ShConfig) Empty() bool {
	return shConfig.ShardName == ""
}

func (mongocfgConfig MongoCfgConfig) Empty() bool {
	return len(mongocfgConfig.Shards) == 0
}

func (rsConfig RsConfig) Validate() error {
	if rsConfig.RsName == "" && len(rsConfig.RsMembers) > 0 || rsConfig.RsName != "" && len(rsConfig.RsMembers) == 0 {
		return errors.Errorf("rsConfig should be all empty or full populated, but rsConfig = %+v", rsConfig)
	}
	if len(rsConfig.RsMembers) > len(rsConfig.RsMemberIDs) {
		return errors.Errorf("not all replica set members have corresponding ID")
	}
	if len(rsConfig.RsMembers) < len(rsConfig.RsMemberIDs) {
		return errors.Errorf("excessive number of replica set IDs")
	}
	return nil
}

func (shConfig ShConfig) Validate() error {
	if (shConfig.ShardName == "") != (shConfig.MongoCfgConnectionString == "") {
		return fmt.Errorf("got shard name %s, but mongocfg connection string is %s", shConfig.ShardName, shConfig.MongoCfgConnectionString)
	}
	return nil
}
