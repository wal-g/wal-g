package binary

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const adminDB = "admin"

const cursorCreateRetries = 10

type MongodService struct {
	Context     context.Context
	MongoClient *mongo.Client
}

func CreateMongodService(ctx context.Context, appName, mongodbURI string) (*MongodService, error) {
	mongoClient, err := mongo.Connect(ctx,
		options.Client().ApplyURI(mongodbURI).
			SetSocketTimeout(time.Minute).
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
	return strings.Contains(err.Error(), "(Location50915)") // mongodb take checkpoint
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

func (mongodService *MongodService) FixSystemDataAfterRestore(LastWriteTS primitive.Timestamp, fixOplog bool) error {
	ctx := mongodService.Context
	localDatabase := mongodService.MongoClient.Database("local")

	err := replaceData(ctx, localDatabase.Collection("replset.election"), true, nil)
	if err != nil {
		return err
	}

	err = replaceData(ctx, localDatabase.Collection("replset.minvalid"), true, bson.M{
		"_id": primitive.NewObjectID(),
		"t":   -1,
		"ts":  primitive.Timestamp{T: 0, I: 1},
	})
	if err != nil {
		return err
	}

	if fixOplog {
		tracelog.DebugLogger.Printf("oplogTruncateAfterPoint: %v", LastWriteTS)
		err = replaceData(ctx, localDatabase.Collection("replset.oplogTruncateAfterPoint"), true,
			bson.M{
				"_id":                     "oplogTruncateAfterPoint",
				"oplogTruncateAfterPoint": LastWriteTS,
			})
		if err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Printf("We are skipping fix replset.oplogTruncateAfterPoint because it is disabled")
	}

	err = replaceData(ctx, localDatabase.Collection("system.replset"), false, nil)
	if err != nil {
		return err
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
