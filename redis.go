package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fzzy/radix/redis"
	datastore "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

var _ datastore.Datastore = (*Datastore)(nil)
var _ datastore.Batching = (*Datastore)(nil)

var ErrInvalidType = errors.New("redis datastore: invalid type error. this datastore only supports []byte values")

func NewExpiringDatastore(client *redis.Client, ttl time.Duration) (*Datastore, error) {
	return &Datastore{
		client: client,
		ttl:    ttl,
	}, nil
}

func NewDatastore(client *redis.Client) (*Datastore, error) {
	return &Datastore{
		client: client,
	}, nil
}

type Datastore struct {
	mu     sync.Mutex
	client *redis.Client
	ttl    time.Duration
}

func (ds *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.client.Append("SET", key.String(), value)
	if ds.ttl != 0 {
		ds.client.Append("EXPIRE", key.String(), ds.ttl.Seconds())
	}
	if err := ds.client.GetReply().Err; err != nil {
		return fmt.Errorf("failed to put value: %s", err)
	}
	if ds.ttl != 0 {
		if err := ds.client.GetReply().Err; err != nil {
			return fmt.Errorf("failed to set expiration: %s", err)
		}
	}
	return nil
}

func (ds *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

func (ds *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.client.Cmd("GET", key.String()).Bytes()
}

func (ds *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.client.Cmd("STRLEN", key.String()).Int()
}

func (ds *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.client.Cmd("EXISTS", key.String()).Bool()
}

func (ds *Datastore) Delete(ctx context.Context, key datastore.Key) (err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.client.Cmd("DEL", key.String()).Err
}

func (ds *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return nil, errors.New("TODO implement query for redis datastore?")
}

func (ds *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return nil, datastore.ErrBatchUnsupported
}

func (ds *Datastore) Close() error {
	return ds.client.Close()
}
