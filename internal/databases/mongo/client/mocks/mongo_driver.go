// Code generated by Yandex patched mockery v1.1.0. DO NOT EDIT.

package clientmocks

import (
	context "context"

	client "github.com/wal-g/wal-g/internal/databases/mongo/client"

	db "github.com/mongodb/mongo-tools/common/db"

	mock "github.com/stretchr/testify/mock"

	models "github.com/wal-g/wal-g/internal/databases/mongo/models"

	primitive "go.mongodb.org/mongo-driver/bson/primitive"
)

// MongoDriver is an autogenerated mock type for the MongoDriver type
type MongoDriver struct {
	mock.Mock
}

// ApplyOp provides a mock function with given fields: ctx, op
func (_m *MongoDriver) ApplyOp(ctx context.Context, op *db.Oplog) error {
	ret := _m.Called(ctx, op)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *db.Oplog) error); ok {
		r0 = rf(ctx, op)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Close provides a mock function with given fields: ctx
func (_m *MongoDriver) Close(ctx context.Context) error {
	ret := _m.Called(ctx)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateIndexes provides a mock function with given fields: ctx, dbName, collName, indexes
func (_m *MongoDriver) CreateIndexes(ctx context.Context, dbName string, collName string, indexes []client.IndexDocument) error {
	ret := _m.Called(ctx, dbName, collName, indexes)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, []client.IndexDocument) error); ok {
		r0 = rf(ctx, dbName, collName, indexes)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DropIndexes provides a mock function with given fields: ctx, dbName, rawCommand
func (_m *MongoDriver) DropIndexes(ctx context.Context, dbName string, rawCommand primitive.D) error {
	ret := _m.Called(ctx, dbName, rawCommand)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, primitive.D) error); ok {
		r0 = rf(ctx, dbName, rawCommand)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// EnsureIsMaster provides a mock function with given fields: ctx
func (_m *MongoDriver) EnsureIsMaster(ctx context.Context) error {
	ret := _m.Called(ctx)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// IsMaster provides a mock function with given fields: ctx
func (_m *MongoDriver) IsMaster(ctx context.Context) (models.IsMaster, error) {
	ret := _m.Called(ctx)

	if len(ret) == 1 {
		rf, ok := ret.Get(0).(func(context.Context) (models.IsMaster, error))
		if ok {
			return rf(ctx)
		}
	}

	var r0 models.IsMaster
	if rf, ok := ret.Get(0).(func(context.Context) models.IsMaster); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Get(0).(models.IsMaster)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// LastWriteTS provides a mock function with given fields: ctx
func (_m *MongoDriver) LastWriteTS(ctx context.Context) (models.Timestamp, models.Timestamp, error) {
	ret := _m.Called(ctx)

	if len(ret) == 1 {
		rf, ok := ret.Get(0).(func(context.Context) (models.Timestamp, models.Timestamp, error))
		if ok {
			return rf(ctx)
		}
	}

	var r0 models.Timestamp
	if rf, ok := ret.Get(0).(func(context.Context) models.Timestamp); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Get(0).(models.Timestamp)
	}

	var r1 models.Timestamp
	if rf, ok := ret.Get(1).(func(context.Context) models.Timestamp); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Get(1).(models.Timestamp)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context) error); ok {
		r2 = rf(ctx)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// TailOplogFrom provides a mock function with given fields: ctx, from
func (_m *MongoDriver) TailOplogFrom(ctx context.Context, from models.Timestamp) (client.OplogCursor, error) {
	ret := _m.Called(ctx, from)

	if len(ret) == 1 {
		rf, ok := ret.Get(0).(func(context.Context, models.Timestamp) (client.OplogCursor, error))
		if ok {
			return rf(ctx, from)
		}
	}

	var r0 client.OplogCursor
	if rf, ok := ret.Get(0).(func(context.Context, models.Timestamp) client.OplogCursor); ok {
		r0 = rf(ctx, from)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(client.OplogCursor)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.Timestamp) error); ok {
		r1 = rf(ctx, from)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
