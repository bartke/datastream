package storage_test

import "github.com/bartke/datastream/storage"

var _ storage.Storage = &storage.S3Storage{}
