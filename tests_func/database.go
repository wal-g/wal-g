package main

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

func SetupDatabase(testContext *TestContextType) {
	conn := EnvDBConnect(testContext, "someName")
	db := conn.Database("someName")
	_, err := db.Collection("targets").InsertOne(context.Background(), map[string]string{
		"status":       "vacant",
		"hostname":     GetVarFromEnvList(testContext.Env, "TARGET_HOST"),
		"ssh_user":     GetVarFromEnvList(testContext.Env, "TARGET_SSH_USER_USER"),
		"ssh_password": GetVarFromEnvList(testContext.Env, "TARGET_SSH_USER_PASSWORD"),
		"ssh_port":     GetVarFromEnvList(testContext.Env, "TARGET_SSH_PORT"),
	})
	if err != nil {
		panic(err)
	}
	indexModel := mongo.IndexModel{
		Keys: bsonx.Doc{{Key: "user", Value: bsonx.Int32(1)}},
		Options: options.Index().SetUnique(true),
	}
	var _, _ = db.Collection("test").Indexes().CreateOne(context.Background(), indexModel)
}
