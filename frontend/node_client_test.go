package frontend_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lyonbrown4d/nespa/cacheapi"
	"github.com/lyonbrown4d/nespa/frontend"
)

func TestNodeClientSetGetDelete(t *testing.T) {
	var seen []string
	server := newNodeClientTestServer(t, &seen)
	defer server.Close()

	client := frontend.NewNodeClient()
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

	assertSeenMethods(t, seen, []string{http.MethodPut, http.MethodGet, http.MethodDelete})
}

func newNodeClientTestServer(t *testing.T, seen *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/node/cache" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		*seen = append(*seen, r.Method)
		w.Header().Set("content-type", "application/json")

		switch r.Method {
		case http.MethodPut:
			handleSetRequest(t, w, r)
		case http.MethodGet:
			handleGetRequest(t, w, r)
		case http.MethodDelete:
			handleDeleteRequest(t, w, r)
		default:
			t.Fatalf("unexpected method: %s", r.Method)
		}
	}))
}

func handleSetRequest(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	var body cacheapi.SetBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode set body: %v", err)
	}
	assertSetBody(t, body)
	writeJSON(t, w, cacheapi.RecordBody{
		Found:     true,
		Namespace: body.Namespace,
		Space:     body.Space,
		Key:       body.Key,
		Value:     body.Value,
		Version:   1,
	})
}

func handleGetRequest(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	assertQueryValue(t, r, "namespace", "ns")
	assertQueryValue(t, r, "space", "sp")
	assertQueryValue(t, r, "key", "k")
	writeJSON(t, w, cacheapi.RecordBody{Found: true, Value: "v"})
}

func handleDeleteRequest(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	assertQueryValue(t, r, "key", "k")
	writeJSON(t, w, cacheapi.DeleteBody{Deleted: true})
}

func assertSetBody(t *testing.T, body cacheapi.SetBody) {
	t.Helper()
	if body.Namespace != "ns" || body.Space != "sp" || body.Key != "k" || body.Value != "v" {
		t.Fatalf("unexpected set body: %+v", body)
	}
}

func assertQueryValue(t *testing.T, r *http.Request, name, want string) {
	t.Helper()
	if got := r.URL.Query().Get(name); got != want {
		t.Fatalf("unexpected %s query: %q", name, got)
	}
}

func assertSeenMethods(t *testing.T, seen, want []string) {
	t.Helper()
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
