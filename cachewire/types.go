// Package cachewire defines Nespa cache TCP wire DTOs.
package cachewire

import (
	"errors"
	"fmt"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/nespa/protocol"
)

var ErrInvalidPayloadRange = errors.New("cachewire: invalid payload range")

type Key struct {
	Namespace string `json:"namespace"`
	Space     string `json:"space"`
	Entity    string `json:"entity,omitempty"`
	Key       string `json:"key"`
}

type SetRequest struct {
	Key
	Value            []byte `json:"-"`
	RouteEpoch       uint64 `json:"-"`
	TTLMillis        int64  `json:"ttl_ms,omitempty"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
	ExpectedVersion  uint64 `json:"expected_version,omitempty"`
	PayloadOffset    uint32 `json:"payload_offset,omitempty"`
	PayloadSize      uint32 `json:"payload_size,omitempty"`
}

type GetRequest struct {
	Key
	RouteEpoch       uint64 `json:"-"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
}

type DeleteRequest struct {
	Key
	RouteEpoch      uint64 `json:"-"`
	ExpectedVersion uint64 `json:"expected_version,omitempty"`
}

type ExistsRequest struct {
	Key
	RouteEpoch       uint64 `json:"-"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
}

type TouchRequest struct {
	Key
	RouteEpoch       uint64 `json:"-"`
	TTLMillis        int64  `json:"ttl_ms"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
	ExpectedVersion  uint64 `json:"expected_version,omitempty"`
}

type AdjustRequest struct {
	Key
	RouteEpoch       uint64 `json:"-"`
	TTLMillis        int64  `json:"ttl_ms,omitempty"`
	InitialValue     int64  `json:"initial_value,omitempty"`
	Delta            int64  `json:"delta"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
	ExpectedVersion  uint64 `json:"expected_version,omitempty"`
}

type BatchSetRequest struct {
	RouteEpoch uint64       `json:"-"`
	Items      []SetRequest `json:"items"`
}

type BatchGetRequest struct {
	RouteEpoch uint64       `json:"-"`
	Items      []GetRequest `json:"items"`
}

type BatchDeleteRequest struct {
	RouteEpoch uint64          `json:"-"`
	Items      []DeleteRequest `json:"items"`
}

type BatchExistsRequest struct {
	RouteEpoch uint64          `json:"-"`
	Items      []ExistsRequest `json:"items"`
}

type BatchTouchRequest struct {
	RouteEpoch uint64         `json:"-"`
	Items      []TouchRequest `json:"items"`
}

type Record struct {
	Found            bool   `json:"found"`
	Namespace        string `json:"namespace,omitempty"`
	Space            string `json:"space,omitempty"`
	Entity           string `json:"entity,omitempty"`
	Key              string `json:"key,omitempty"`
	Value            []byte `json:"-"`
	Version          uint64 `json:"version,omitempty"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
	ExpireAtUnixMs   int64  `json:"expire_at_ms,omitempty"`
	PayloadOffset    uint32 `json:"payload_offset,omitempty"`
	PayloadSize      uint32 `json:"payload_size,omitempty"`
}

type DeleteResponse struct {
	Deleted bool `json:"deleted"`
}

type ExistsResponse struct {
	Exists bool `json:"exists"`
}

type TouchResponse struct {
	Touched bool `json:"touched"`
}

type BatchSetResponse struct {
	Records []Record `json:"records"`
}

type BatchGetResponse struct {
	Records []Record `json:"records"`
}

type BatchDeleteResponse struct {
	Results []DeleteResponse `json:"results"`
}

type BatchExistsResponse struct {
	Results []ExistsResponse `json:"results"`
}

type BatchTouchResponse struct {
	Results []TouchResponse `json:"results"`
}

type Error struct {
	Code    protocol.ErrorCode `json:"code"`
	Message string             `json:"message"`
}

func (e Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("cache tcp error: code %d", e.Code)
	}
	return "cache tcp error: " + e.Message
}

func PackBatchSet(request BatchSetRequest) (BatchSetRequest, []byte, error) {
	items := collectionlist.NewListWithCapacity[SetRequest](len(request.Items))
	payload := make([]byte, 0, payloadSize(request.Items))
	for index := range request.Items {
		item := request.Items[index]
		offset, size, err := checkedPayloadRange(len(payload), len(item.Value))
		if err != nil {
			return BatchSetRequest{}, nil, err
		}
		item.PayloadOffset = offset
		item.PayloadSize = size
		payload = append(payload, item.Value...)
		item.Value = nil
		items.Add(item)
	}
	return BatchSetRequest{RouteEpoch: request.RouteEpoch, Items: items.Values()}, payload, nil
}

func UnpackBatchSet(request BatchSetRequest, payload []byte) ([]SetRequest, error) {
	items := collectionlist.NewListWithCapacity[SetRequest](len(request.Items))
	for index := range request.Items {
		item := request.Items[index]
		value, err := SlicePayload(payload, item.PayloadOffset, item.PayloadSize)
		if err != nil {
			return nil, err
		}
		item.Value = value
		items.Add(item)
	}
	return items.Values(), nil
}

func PackRecords(records []Record) (BatchGetResponse, []byte, error) {
	items := collectionlist.NewListWithCapacity[Record](len(records))
	payload := make([]byte, 0, recordPayloadSize(records))
	for index := range records {
		record := records[index]
		if record.Found {
			offset, size, err := checkedPayloadRange(len(payload), len(record.Value))
			if err != nil {
				return BatchGetResponse{}, nil, err
			}
			record.PayloadOffset = offset
			record.PayloadSize = size
			payload = append(payload, record.Value...)
		}
		record.Value = nil
		items.Add(record)
	}
	return BatchGetResponse{Records: items.Values()}, payload, nil
}

func UnpackRecords(response BatchGetResponse, payload []byte) ([]Record, error) {
	records := collectionlist.NewListWithCapacity[Record](len(response.Records))
	for index := range response.Records {
		record := response.Records[index]
		if record.Found {
			value, err := SlicePayload(payload, record.PayloadOffset, record.PayloadSize)
			if err != nil {
				return nil, err
			}
			record.Value = value
		}
		records.Add(record)
	}
	return records.Values(), nil
}

func SlicePayload(payload []byte, offset, size uint32) ([]byte, error) {
	start := int(offset)
	end := start + int(size)
	if start < 0 || end < start || end > len(payload) {
		return nil, fmt.Errorf("%w: offset=%d size=%d payload=%d", ErrInvalidPayloadRange, offset, size, len(payload))
	}
	return append([]byte(nil), payload[start:end]...), nil
}

func payloadSize(items []SetRequest) int {
	var total int
	for index := range items {
		total += len(items[index].Value)
	}
	return total
}

func recordPayloadSize(records []Record) int {
	var total int
	for index := range records {
		total += len(records[index].Value)
	}
	return total
}

func checkedPayloadRange(offset, size int) (uint32, uint32, error) {
	const maxUint32 = uint64(^uint32(0))
	if offset < 0 || uint64(offset) > maxUint32 {
		return 0, 0, fmt.Errorf("%w: offset %d > %d", ErrInvalidPayloadRange, offset, maxUint32)
	}
	if size < 0 || uint64(size) > maxUint32 {
		return 0, 0, fmt.Errorf("%w: size %d > %d", ErrInvalidPayloadRange, size, maxUint32)
	}
	return uint32(offset), uint32(size), nil
}
