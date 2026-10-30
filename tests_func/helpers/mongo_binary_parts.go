package helpers

import (
	"fmt"
	"math/rand"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (mc *MongoCtl) WriteBinaryBackupPartsData(documentCount int) error {
	conn, err := mc.AdminConnect()
	if err != nil {
		return err
	}
	collection := conn.Database("file_parts").Collection("documents")
	random := rand.New(rand.NewSource(42))
	randomString := func(length int) string {
		const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		value := make([]byte, length)
		for i := range value {
			value[i] = alphabet[random.Intn(len(alphabet))]
		}
		return string(value)
	}

	const batchSize = 500
	for first := 0; first < documentCount; first += batchSize {
		documents := make([]interface{}, 0, batchSize)
		for id := first; id < min(first+batchSize, documentCount); id++ {
			documents = append(documents, bson.M{
				"_id":     fmt.Sprintf("%08d-%s", id, randomString(64)),
				"key":     randomString(128),
				"group":   id % 101,
				"number":  id,
				"tags":    bson.A{randomString(64), randomString(64), randomString(64)},
				"payload": randomString(2048),
			})
		}
		if _, err = collection.InsertMany(mc.ctx, documents); err != nil {
			return err
		}
	}

	_, err = collection.Indexes().CreateMany(mc.ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "key", Value: 1}}, Options: options.Index().SetName("unique_key").SetUnique(true)},
		{Keys: bson.D{{Key: "group", Value: 1}, {Key: "key", Value: -1}},
			Options: options.Index().SetName("group_key")},
		{Keys: bson.D{{Key: "tags", Value: 1}}, Options: options.Index().SetName("tags")},
	})
	if err != nil {
		return err
	}
	if _, err = conn.Database("file_parts").Collection("excluded").InsertOne(mc.ctx, bson.M{"_id": 1}); err != nil {
		return err
	}
	return conn.Database(AdminDB).RunCommand(mc.ctx, bson.D{{Key: "fsync", Value: 1}}).Err()
}

func (mc *MongoCtl) ValidateBinaryBackupPartsData(documentCount int) error {
	conn, err := mc.AdminConnect()
	if err != nil {
		return err
	}
	database := conn.Database("file_parts")
	count, err := database.Collection("documents").CountDocuments(mc.ctx, bson.D{})
	if err != nil {
		return err
	}
	if count != int64(documentCount) {
		return fmt.Errorf("expected %d documents, got %d", documentCount, count)
	}
	var validation struct {
		Valid    bool     `bson:"valid"`
		NIndexes int      `bson:"nIndexes"`
		Errors   []string `bson:"errors"`
	}
	if err = database.RunCommand(mc.ctx, bson.D{
		{Key: "validate", Value: "documents"}, {Key: "full", Value: true},
	}).Decode(&validation); err != nil {
		return err
	}
	if !validation.Valid || validation.NIndexes != 4 || len(validation.Errors) != 0 {
		return fmt.Errorf("collection validation failed: %+v", validation)
	}
	return nil
}
