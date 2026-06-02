package cachewire_test

import (
	"strconv"
	"testing"

	"github.com/lyonbrown4d/nespa/cachewire"
)

var (
	benchSetRequest       cachewire.SetRequest
	benchBatchSetRequest  cachewire.BatchSetRequest
	benchBatchGetResponse cachewire.BatchGetResponse
	benchMetadata         []byte
	benchPayload          []byte
)

func BenchmarkBinarySetRequestEncodeDecode(b *testing.B) {
	request := cachewire.SetRequest{
		Key:              benchmarkWireKey("set", 1),
		TTLMillis:        60_000,
		NamespaceVersion: 2,
		SpaceVersion:     3,
		ExpectedVersion:  4,
	}

	b.ReportAllocs()
	for range b.N {
		metadata := cachewire.EncodeSetRequest(request)
		out, err := cachewire.DecodeSetRequest(metadata)
		if err != nil {
			b.Fatalf("decode set request: %v", err)
		}
		benchSetRequest = out
		benchMetadata = metadata
	}
}

func BenchmarkBinaryBatchSetRequestEncodeDecode(b *testing.B) {
	request := cachewire.BatchSetRequest{Items: benchmarkWireSetItems(16)}

	b.ReportAllocs()
	for range b.N {
		metadata, payload, err := cachewire.EncodeBatchSetRequest(request)
		if err != nil {
			b.Fatalf("encode batch set request: %v", err)
		}
		out, err := cachewire.DecodeBatchSetRequest(metadata, payload)
		if err != nil {
			b.Fatalf("decode batch set request: %v", err)
		}
		benchBatchSetRequest = out
		benchMetadata = metadata
		benchPayload = payload
	}
}

func BenchmarkBinaryBatchGetResponseEncodeDecode(b *testing.B) {
	response := cachewire.BatchGetResponse{Records: benchmarkWireRecords(16)}

	b.ReportAllocs()
	for range b.N {
		metadata, payload, err := cachewire.EncodeBatchGetResponse(response)
		if err != nil {
			b.Fatalf("encode batch get response: %v", err)
		}
		out, err := cachewire.DecodeBatchGetResponse(metadata, payload)
		if err != nil {
			b.Fatalf("decode batch get response: %v", err)
		}
		benchBatchGetResponse = out
		benchMetadata = metadata
		benchPayload = payload
	}
}

func benchmarkWireSetItems(count int) []cachewire.SetRequest {
	items := make([]cachewire.SetRequest, 0, count)
	for index := range count {
		items = append(items, cachewire.SetRequest{
			Key:              benchmarkWireKey("set", index),
			Value:            []byte("benchmark-value-" + strconv.Itoa(index)),
			TTLMillis:        60_000,
			NamespaceVersion: 2,
			SpaceVersion:     3,
			ExpectedVersion:  4,
		})
	}
	return items
}

func benchmarkWireRecords(count int) []cachewire.Record {
	records := make([]cachewire.Record, 0, count)
	for index := range count {
		records = append(records, cachewire.Record{
			Found:            true,
			Namespace:        "orders",
			Space:            "session",
			Entity:           "OrderSession",
			Key:              "record-" + strconv.Itoa(index),
			Value:            []byte("benchmark-value-" + strconv.Itoa(index)),
			Version:          uint64(index + 1),
			NamespaceVersion: 2,
			SpaceVersion:     3,
		})
	}
	return records
}

func benchmarkWireKey(prefix string, index int) cachewire.Key {
	return cachewire.Key{
		Namespace: "orders",
		Space:     "session",
		Entity:    "OrderSession",
		Key:       prefix + "-" + strconv.Itoa(index),
	}
}
