package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
