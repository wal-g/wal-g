package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestMongoOplogCursor_NextPush(t *testing.T) {
	ctx := t.Context()
	m := NewMongoOplogCursor(&mongo.Cursor{})
	assert.Nil(t, m.Push([]byte{'t'}))
	assert.EqualError(t, m.Push([]byte{'e'}), "cursor already has one unread pushed document")
	assert.Equal(t, m.pushed, []byte{'t'})

	assert.True(t, m.Next(ctx))
	assert.Equal(t, m.Data(), []byte{'t'})
	assert.Nil(t, m.pushed)

	assert.Panics(t, func() { m.Next(ctx) })
}

func TestBsonCursor_NextPush(t *testing.T) {
	ctx := t.Context()
	m := NewBsonCursor(nil)

	assert.Nil(t, m.Push([]byte{'t'}))
	assert.EqualError(t, m.Push([]byte{'e'}), "cursor already has one unread pushed document")
	assert.Equal(t, m.pushed, []byte{'t'})
	assert.True(t, m.Next(ctx))
	assert.Equal(t, m.Data(), []byte{'t'})
	assert.Nil(t, m.pushed)

	assert.False(t, m.Next(ctx))
}

func TestApplyOpsResponseChecksEveryOperation(t *testing.T) {
	cases := []struct {
		name    string
		results bson.A
		applied int
		wantErr string
	}{
		{name: "success", results: bson.A{true, true}, applied: 2},
		{name: "partial failure", results: bson.A{true, false}, applied: 2, wantErr: "entry 2 of 2"},
		{name: "short response", results: bson.A{true}, applied: 1, wantErr: "applied 1 of 2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := bson.Marshal(bson.D{
				{Key: "ok", Value: 1},
				{Key: "applied", Value: tc.applied},
				{Key: "results", Value: tc.results},
			})
			require.NoError(t, err)
			var response applyOpsResponse
			require.NoError(t, bson.Unmarshal(raw, &response))
			if tc.wantErr == "" {
				require.NoError(t, response.check(2))
			} else {
				require.ErrorContains(t, response.check(2), tc.wantErr)
			}
		})
	}
}
