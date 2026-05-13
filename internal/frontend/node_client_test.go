package frontend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lyonbrown4d/nespa/internal/cacheapi"
)

func TestNodeClientSetGetDelete(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != nodeCachePath {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		seen = append(seen, r.Method)
		w.Header().Set("content-type", "application/json")

		switch r.Method {
		case http.MethodPut:
			var body cacheapi.SetBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode set body: %v", err)
			}
			if body.Namespace != "ns" || body.Space != "sp" || body.Key != "k" || body.Value != "v" {
				t.Fatalf("unexpected set body: %+v", body)
			}
			writeJSON(t, w, cacheapi.RecordBody{
				Found:     true,
				Namespace: body.Namespace,
				Space:     body.Space,
				Key:       body.Key,
				Value:     body.Value,
				Version:   1,
			})
		case http.MethodGet:
			if got := r.URL.Query().Get("namespace"); got != "ns" {
				t.Fatalf("unexpected get namespace: %q", got)
			}
			if got := r.URL.Query().Get("space"); got != "sp" {
				t.Fatalf("unexpected get space: %q", got)
			}
			if got := r.URL.Query().Get("key"); got != "k" {
				t.Fatalf("unexpected get key: %q", got)
			}
			writeJSON(t, w, cacheapi.RecordBody{Found: true, Value: "v"})
		case http.MethodDelete:
			if got := r.URL.Query().Get("key"); got != "k" {
				t.Fatalf("unexpected delete key: %q", got)
			}
			writeJSON(t, w, cacheapi.DeleteBody{Deleted: true})
		default:
			t.Fatalf("unexpected method: %s", r.Method)
		}
	}))
	defer server.Close()

	client := NewNodeClient()
	ctx := context.Background()

	set, err := client.Set(ctx, server.URL, cacheapi.SetBody{Namespace: "ns", Space: "sp", Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if !set.Found || set.Version != 1 {
		t.Fatalf("unexpected set response: %+v", set)
	}

	get, err := client.Get(ctx, server.URL, cacheapi.GetInput{Namespace: "ns", Space: "sp", Key: "k"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !get.Found || get.Value != "v" {
		t.Fatalf("unexpected get response: %+v", get)
	}

	del, err := client.Delete(ctx, server.URL, cacheapi.DeleteInput{Namespace: "ns", Space: "sp", Key: "k"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !del.Deleted {
		t.Fatalf("unexpected delete response: %+v", del)
	}

	want := []string{http.MethodPut, http.MethodGet, http.MethodDelete}
	for i, method := range want {
		if seen[i] != method {
			t.Fatalf("method[%d] = %s, want %s", i, seen[i], method)
		}
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write json: %v", err)
	}
}
