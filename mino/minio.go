package minio

import (
	"sync"

	sminio "github.com/minio/minio-go/v6"
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
	client *sminio.Client
	bucket *sminio.BucketInfo
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
		o.client, initErr = sminio.New(config.Endpoint, config.AccessKeyId, config.AccessKeySecret, config.IsSsl)
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
