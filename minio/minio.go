package minio

import (
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

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

func (m *minio) Put(key string, r io.Reader, dataLength int64, contentType string) error {
	key = storage.NormalizeKey(key)
	_, err := m.client.PutObject(m.config.Bucket, key, r, dataLength, minioStorage.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return err
	}
	return nil
}

func (m *minio) PutFile(key string, localFile string, contentType string) error {
	key = storage.NormalizeKey(key)
	_, err := m.client.FPutObject(m.config.Bucket, key, localFile, minioStorage.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return err
	}
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
	err := m.Copy(srcKey, destKey)
	if err != nil {
		return err
	}
	err = m.Delete(srcKey)
	if err != nil {
		return err
	}
	return nil
}

func (m *minio) Copy(srcKey string, destKey string) error {
	srcKey = storage.NormalizeKey(srcKey)
	destKey = storage.NormalizeKey(destKey)
	src := minioStorage.NewSourceInfo(m.config.Bucket, srcKey, nil)
	dst, err := minioStorage.NewDestinationInfo(m.config.Bucket, destKey, nil, nil)
	if err != nil {
		return err
	}

	err = m.client.CopyObject(dst, src)
	if err != nil {
		return err
	}
	return nil
}

func (m *minio) Exists(key string) (bool, error) {
	key = storage.NormalizeKey(key)
	_, err := m.client.GetObject(m.config.Bucket, key, minioStorage.GetObjectOptions{})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (m *minio) Size(key string) (int64, error) {
	key = storage.NormalizeKey(key)
	object, err := m.client.GetObject(m.config.Bucket, key, minioStorage.GetObjectOptions{})
	if err != nil {
		return 0, err
	}
	var b []byte
	size, err := object.Read(b)
	if err != nil {
		return 0, err
	}
	return int64(size), nil
}

func (m *minio) Delete(key string) error {
	key = storage.NormalizeKey(key)
	err := m.client.RemoveObject(m.config.Bucket, key)
	if err != nil {
		return err
	}

	return nil
}

func (m *minio) Url(key string) string {
	key = storage.NormalizeKey(key)
	reqParams := make(url.Values)
	presignedURL, err := m.client.PresignedGetObject(m.config.Bucket, key, time.Second*24*60*60, reqParams)
	if err != nil {
		return ""
	}
	return presignedURL.String()
}
