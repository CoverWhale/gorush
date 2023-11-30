package nats

import (
	"context"
	"fmt"
	"strconv"

	"github.com/appleboy/gorush/config"
	"github.com/appleboy/gorush/logx"
	"github.com/nats-io/nats.go"
)

// New func implements the storage interface for gorush (https://github.com/appleboy/gorush)
func New(config *config.ConfYaml) *Storage {
	return &Storage{
		ctx:    context.Background(),
		config: config,
		bucket: config.Queue.NATS.Bucket,
		url:    config.Queue.NATS.Addr,
	}
}

// Storage is interface structure
type Storage struct {
	ctx    context.Context
	config *config.ConfYaml
	client *nats.Conn
	js     nats.JetStreamContext
	bucket string
	url    string
}

func (s *Storage) Add(key string, count int64) {
	kv, err := s.js.KeyValue(s.bucket)
	if err != nil {
		logx.LogAccess.Error(err)
		return
	}
	_, err = kv.Put(key, []byte(strconv.Itoa(int(count))))
	if err != nil {
		logx.LogAccess.Error(err)
	}
}

func (s *Storage) Set(key string, count int64) {
	kv, err := s.js.KeyValue(s.bucket)
	if err != nil {
		logx.LogAccess.Error(err)
		return
	}
	_, err = kv.Put(key, []byte(strconv.Itoa(int(count))))
	if err != nil {
		logx.LogAccess.Error(err)
	}
}

func (s *Storage) Get(key string) int64 {
	kv, err := s.js.KeyValue(s.bucket)
	if err != nil {
		logx.LogAccess.Error(err)
	}

	v, err := kv.Get(key)
	if err != nil {
		logx.LogAccess.Error(err)
	}

	count, err := strconv.ParseInt(string(v.Value()), 10, 64)
	if err != nil {
		logx.LogAccess.Error(err)
	}

	return count
}

// Init client storage.
func (s *Storage) Init() error {
	nc, err := nats.Connect(s.url)
	if err != nil {
		return err
	}

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	s.client = nc
	s.js = js

	stream := fmt.Sprintf("KV_%s", s.bucket)

	_, err = js.StreamInfo(stream)
	if err != nil && err != nats.ErrStreamNotFound {
		return err
	}

	if err != nil && err == nats.ErrStreamNotFound {
		kvCfg := nats.KeyValueConfig{
			Bucket:      s.bucket,
			Replicas:    1,
			Description: "gorush NATS Bucket",
			History:     3,
		}

		_, err := js.CreateKeyValue(&kvCfg)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close the storage connection
func (s *Storage) Close() error {
	s.client.Close()
	return nil
}
