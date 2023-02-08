package service

import (
	"context"

	"github.com/bartke/datastream/generated/datastream"
	"github.com/bartke/datastream/storage"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DataServiceServer struct {
	store storage.Storage
	datastream.UnimplementedDataServiceServer
}

func NewDataServiceServer(store storage.Storage) *DataServiceServer {
	return &DataServiceServer{
		store: store,
	}
}

func (s *DataServiceServer) ListCapabilities(ctx context.Context, in *datastream.ListCapabilitiesRequest) (*datastream.ListCapabilitiesResponse, error) {
	capabilities, err := s.store.ListCapabilities()
	if err != nil {
		return nil, err
	}

	resp := &datastream.ListCapabilitiesResponse{
		Capabilities: make([]*datastream.Capability, len(capabilities)),
	}
	for i, cap := range capabilities {
		resp.Capabilities[i] = &datastream.Capability{
			Key:       cap.Key,
			ValueType: cap.ValueType,
		}
	}
	return resp, nil
}

func (s *DataServiceServer) Sync(ctx context.Context, in *datastream.DataRequest) (*datastream.DataResponse, error) {
	data, err := s.store.Sync(in.Keys)
	if err != nil {
		return nil, err
	}

	resp := &datastream.DataResponse{
		Data: make(map[string]*datastream.Data),
	}
	for k, v := range data {
		resp.Data[k] = &datastream.Data{
			Key:       v.Key,
			Value:     v.Value,
			ValueType: v.ValueType,
			UpdatedAt: timestamppb.New(v.UpdatedAt),
		}
	}
	return resp, nil
}

func (s *DataServiceServer) Subscribe(in *datastream.DataRequest, stream datastream.DataService_SubscribeServer) error {
	updates, err := s.store.Subscribe(in.Keys)
	if err != nil {
		return err
	}

	for update := range updates {
		response := &datastream.DataResponse{
			Data: map[string]*datastream.Data{
				update.Key: {
					Key:       update.Key,
					Value:     update.Value,
					ValueType: update.ValueType,
					UpdatedAt: timestamppb.New(update.UpdatedAt),
				},
			},
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

func (s *DataServiceServer) PushUpdate(ctx context.Context, in *datastream.Data) (*empty.Empty, error) {
	data := &storage.Data{
		Key:       in.Key,
		Value:     in.Value,
		ValueType: in.ValueType,
		UpdatedAt: in.UpdatedAt.AsTime(),
	}
	if err := s.store.PushUpdate(data); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}
