package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
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
}

// NewS3Storage creates a new S3Storage instance
func NewS3Storage(endpoint, region, accessKey, secretKey, bucket string, syncInterval time.Duration) (*S3Storage, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	return &S3Storage{
		client:       s3.New(sess),
		bucket:       bucket,
		syncInterval: syncInterval,
	}, nil
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
	// last := make(map[string]time.Time)
	lastETag := make(map[string]string)

	go func() {
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
						log.Println("Error listing objects in S3:", err)
						continue
					}

					// Loop through the objects and check if they have been updated
					for _, obj := range resp.Contents {
						// Check if the object has been updated by comparing its ETAG
						fmt.Println("obj.ETag", *obj.ETag)
						if lastETag[*obj.Key] != *obj.ETag {
							s.fetchObjectAndSendUpdate(updates, *obj.Key)
							lastETag[*obj.Key] = *obj.ETag
						}
					}
				} else {
					head, err := s.client.HeadObject(&s3.HeadObjectInput{
						Bucket: aws.String(s.bucket),
						Key:    aws.String(key),
					})
					if err != nil || head.ETag == nil {
						log.Println("Error getting object from S3:", err)
						continue
					}

					fmt.Println("head.ETag", *head.ETag)
					if lastETag[key] != *head.ETag {
						s.fetchObjectAndSendUpdate(updates, key)
						lastETag[key] = *head.ETag
					}
				}

				<-time.After(s.syncInterval)
			}
		}
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
