package storage

import "time"

type Capability struct {
	Key       string
	ValueType string
}

type Data struct {
	Key       string
	Value     []byte
	ValueType string
	UpdatedAt time.Time
}

type Storage interface {
	// ListCapabilities returns a list of capabilities (keys and their data types) that can be subscribed to
	ListCapabilities() ([]Capability, error)

	// Sync retrieves the current state of the specified keys
	Sync(keys []string) (map[string]Data, error)

	// Subscribe returns a channel that will receive updates for the specified keys
	Subscribe(keys []string) (<-chan Data, error)

	// PushUpdate stores an updated value for a key
	PushUpdate(data *Data) error
}
