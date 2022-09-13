package minio

import (
	"io"
	"net/http"
	"sync"

	minioStorage "github.com/minio/minio-go/v6"
	"github.com/taokunTeam/go-storage/storage"
)

type Config struct {
	AccessKeyId     string `mapstructure:"access_key_id" json:"access_key_id" yaml:"access_key_id"`
	AccessKeySecret string `mapstructure:"access_key_secret" json:"access_key_secret" yaml:"access_key_secret"`
	Bucket          string `mapstructure:"bucket" json:"bucket" yaml:"bucket"`
	Endpoint        string `mapstructure:"endpoint" json:"endpoint" yaml:"endpoint"`
	IsSsl           bool   `mapstructure:"is_ssl" json:"is_ssl" yaml:"is_ssl"`
	IsPrivate       bool   `mapstructure:"is_private" json:"is_private" yaml:"is_private"`
}

type minio struct {
	config *Config
	client *minioStorage.Client
	bucket *minioStorage.BucketInfo
}

var (
	o       *minio
	once    *sync.Once
	initErr error
)

func Init(config Config) (storage.Storage, error) {
	once = &sync.Once{}
	once.Do(func() {
		o = &minio{}
		o.config = &config
		o.client, initErr = minioStorage.New(config.Endpoint, config.AccessKeyId, config.AccessKeySecret, config.IsSsl)
		if initErr != nil {
			return
		}
		storage.Register(storage.Minio, o)
	})
	if initErr != nil {
		return nil, initErr
	}
	return o, nil
}

func (m *minio) Put(key string, r io.Reader, dataLength int64) error {
	key = storage.NormalizeKey(key)
	return nil
}

func (m *minio) PutFile(key string, localFile string) error {
	key = storage.NormalizeKey(key)
	return nil
}

func (m *minio) Get(key string) (io.ReadCloser, error) {
	key = storage.NormalizeKey(key)

	resp, err := http.Get(m.Url(key))
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (m *minio) Rename(srcKey string, destKey string) error {
	srcKey = storage.NormalizeKey(srcKey)
	destKey = storage.NormalizeKey(destKey)

	return nil
}

func (m *minio) Copy(srcKey string, destKey string) error {
	srcKey = storage.NormalizeKey(srcKey)
	destKey = storage.NormalizeKey(destKey)

	return nil
}

func (m *minio) Exists(key string) (bool, error) {
	key = storage.NormalizeKey(key)

	return true, nil
}

func (m *minio) Size(key string) (int64, error) {
	key = storage.NormalizeKey(key)
	return 0, nil
}

func (m *minio) Delete(key string) error {
	key = storage.NormalizeKey(key)

	return nil
}

func (m *minio) Url(key string) string {
	return ""
}
