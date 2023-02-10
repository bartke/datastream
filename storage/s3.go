package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Storage implements the datastream.Storage interface for S3-compatible storage
type S3Storage struct {
	client       *s3.S3
	bucket       string
	syncInterval time.Duration
	errorChannel chan<- error
}

type S3StorageConfig struct {
	// S3 compatible storage endpoint
	Endpoint string
	// S3 compatible storage region
	Region string
	// S3 compatible storage access key
	AccessKey string
	// S3 compatible storage secret key
	SecretKey string
	// S3 compatible storage bucket
	Bucket string

	// optional aws access token
	AccessToken string

	// optional sync interval, default is 5 seconds
	SyncInterval time.Duration

	// optional error channel
	ErrorChan chan<- error
}

// NewS3Storage creates a new S3Storage instance
func NewS3Storage(config S3StorageConfig) (*S3Storage, error) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(config.Endpoint),
		Region:           aws.String(config.Region),
		Credentials:      credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, config.AccessToken),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	if config.SyncInterval == 0 {
		config.SyncInterval = DefaultSyncInterval
	}

	return &S3Storage{
		client:       s3.New(sess),
		bucket:       config.Bucket,
		syncInterval: config.SyncInterval,
		errorChannel: config.ErrorChan,
	}, nil
}

func (s *S3Storage) forwardError(err error) {
	if s.errorChannel != nil {
		s.errorChannel <- err
	}
}

// ListCapabilities lists available keys for subscription
func (s *S3Storage) ListCapabilities() ([]Capability, error) {
	var capabilities []Capability

	// List all objects in the S3 bucket
	result, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return nil, err
	}

	// Add the root directory as a capability
	capabilities = append(capabilities, Capability{
		Key:       "/",
		ValueType: "directory",
	})

	// Add each object key as a capability
	for _, object := range result.Contents {
		capabilities = append(capabilities, Capability{
			Key:       *object.Key,
			ValueType: "binary", // Assume all objects in S3 are binary data
		})
	}

	return capabilities, nil
}

func (s *S3Storage) Sync(keys []string) (map[string]Data, error) {

	data := make(map[string]Data)
	for _, key := range keys {
		obj, err := s.client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve object %s from bucket %s: %v", key, s.bucket, err)
		}

		value, err := ioutil.ReadAll(obj.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read contents of object %s from bucket %s: %v", key, s.bucket, err)
		}

		data[key] = Data{
			Key:       key,
			Value:     value,
			ValueType: "binary", // Assume all objects in S3 are binary data
			UpdatedAt: time.Now(),
		}
	}
	return data, nil
}

func (s *S3Storage) Subscribe(keys []string) (<-chan Data, error) {
	updates := make(chan Data)
	lastETag := make(map[string]string)

	go func() {
	outer:
		for {
			for _, key := range keys {
				if strings.HasSuffix(key, "/") {
					// The key is a directory, fetch the files periodically

					// List the objects in the directory
					resp, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
						Bucket:  aws.String(s.bucket),
						Prefix:  aws.String(key),
						MaxKeys: aws.Int64(1000), // fixme
					})
					if err != nil {
						s.forwardError(err)
						break outer
					}

					// Loop through the objects and check if they have been updated
					for _, obj := range resp.Contents {
						// Check if the object has been updated by comparing its ETAG
						if lastETag[*obj.Key] == *obj.ETag {
							continue
						}

						err := s.fetchObjectAndSendUpdate(updates, *obj.Key)
						if err != nil {
							s.forwardError(err)
							continue
						}

						lastETag[*obj.Key] = *obj.ETag
					}
				} else {
					head, err := s.client.HeadObject(&s3.HeadObjectInput{
						Bucket: aws.String(s.bucket),
						Key:    aws.String(key),
					})
					if err != nil || head.ETag == nil {
						s.forwardError(err)
						break outer
					}

					if lastETag[key] != *head.ETag {
						s.fetchObjectAndSendUpdate(updates, key)
						lastETag[key] = *head.ETag
					}
				}

				<-time.After(s.syncInterval)
			}
		}
		close(updates)
	}()

	return updates, nil
}

func (s *S3Storage) fetchObjectAndSendUpdate(updates chan Data, key string) error {
	// Fetch the object from S3
	getResp, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	// The object has been updated, send an update
	data, err := ioutil.ReadAll(getResp.Body)
	if err != nil {
		return err
	}

	updates <- Data{
		Key:       key,
		Value:     data,
		ValueType: "binary",
		UpdatedAt: time.Now(),
	}

	return nil
}

func (s *S3Storage) PushUpdate(data *Data) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(data.Key),
		Body:   bytes.NewReader(data.Value),
	})
	if err != nil {
		return err
	}

	return nil
}
