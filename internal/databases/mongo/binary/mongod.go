package binary

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const adminDB = "admin"

type MongodService struct {
	Context     context.Context
	MongoClient *mongo.Client
}

func CreateMongodService(ctx context.Context, appName string) (*MongodService, error) {
	mongodbURI, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
	if err != nil {
		return nil, err
	}

	mongoClient, err := mongo.Connect(ctx,
		options.Client().ApplyURI(mongodbURI).
			SetAppName(appName).
			SetDirect(true).
			SetRetryReads(false))
	if err != nil {
		return nil, err
	}
	err = mongoClient.Ping(ctx, nil)
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

func (mongodService *MongodService) GetBackupCursor() (*mongo.Cursor, error) {
	return mongodService.MongoClient.Database(adminDB).Aggregate(mongodService.Context, mongo.Pipeline{
		{{Key: "$backupCursor", Value: bson.D{}}},
	})
}

//nolint: whitespace
func (mongodService *MongodService) GetBackupCursorExtended(backupID *primitive.Binary,
	lastTS primitive.Timestamp) (*mongo.Cursor, error) {

	return mongodService.MongoClient.Database(adminDB).Aggregate(mongodService.Context, mongo.Pipeline{
		{{
			Key: "$backupCursorExtend", Value: bson.D{
				{Key: "backupId", Value: backupID},
				{Key: "timestamp", Value: lastTS},
			},
		}},
	})
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
