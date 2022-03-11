package redis

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/fzzy/radix/redis"
	datastore "github.com/ipfs/go-datastore"

	dstest "github.com/ipfs/go-datastore/test"
)

const RedisEnv = "REDIS_DATASTORE_TEST_HOST"

var bg = context.Background()

func TestPutGetBytes(t *testing.T) {
	client := clientOrAbort(t)
	ds, err := NewDatastore(client)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("foo"), []byte("bar")
	dstest.Nil(ds.Put(bg, key, val), t)
	v, err := ds.Get(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, val) {
		t.Fail()
	}
}

func TestHasBytes(t *testing.T) {
	client := clientOrAbort(t)
	ds, err := NewDatastore(client)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("foo"), []byte("bar")
	has, err := ds.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fail()
	}

	dstest.Nil(ds.Put(bg, key, val), t)
	hasAfterPut, err := ds.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if !hasAfterPut {
		t.Fail()
	}
}

func TestGetSize(t *testing.T) {
	client := clientOrAbort(t)
	ds, err := NewDatastore(client)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("foo"), []byte("bar")
	_, err = ds.GetSize(bg, key)
	if err != nil {
		t.Fatal(err)
	}

	dstest.Nil(ds.Put(bg, key, val), t)
	size, err := ds.GetSize(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if size != len("bar") {
		t.Fail()
	}
}

func TestDelete(t *testing.T) {
	client := clientOrAbort(t)
	ds, err := NewDatastore(client)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("foo"), []byte("bar")
	dstest.Nil(ds.Put(bg, key, val), t)
	dstest.Nil(ds.Delete(bg, key), t)

	hasAfterDelete, err := ds.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if hasAfterDelete {
		t.Fail()
	}
}

func TestExpiry(t *testing.T) {
	ttl := 1 * time.Second
	client := clientOrAbort(t)
	ds, err := NewExpiringDatastore(client, ttl)
	if err != nil {
		t.Fatal(err)
	}
	key, val := datastore.NewKey("foo"), []byte("bar")
	dstest.Nil(ds.Put(bg, key, val), t)
	time.Sleep(ttl + 1*time.Second)
	dstest.Nil(ds.Delete(bg, key), t)

	hasAfterExpiration, err := ds.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if hasAfterExpiration {
		t.Fail()
	}
}

func clientOrAbort(t *testing.T) *redis.Client {
	c, err := redis.Dial("tcp", os.Getenv(RedisEnv))
	if err != nil {
		t.Log("could not connect to a redis instance")
		t.SkipNow()
	}
	if err := c.Cmd("FLUSHALL").Err; err != nil {
		t.Fatal(err)
	}
	return c
}
