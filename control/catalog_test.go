package control_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lyonbrown4d/nespa/control"
	"github.com/lyonbrown4d/nespa/controlapi"
)

func TestControlStateCreatesNamespaceAndSpaceMetadata(t *testing.T) {
	state := control.NewControlStateWithClock("test", func() time.Time { return time.Unix(123, 0) })

	namespace, err := state.CreateNamespace(" orders ")
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	assertNamespaceResponse(t, namespace, 1, "orders")

	again, err := state.CreateNamespace("orders")
	if err != nil {
		t.Fatalf("create namespace again: %v", err)
	}
	assertNamespaceResponse(t, again, 1, "orders")

	space, err := state.CreateSpace("orders", "session")
	if err != nil {
		t.Fatalf("create space: %v", err)
	}
	assertSpaceResponse(t, space, 2, "orders", "session")

	requireCatalogCounts(t, state.Namespaces(), state.Spaces(), 1, 1)
}

func TestControlStateCreateSpaceRequiresNamespace(t *testing.T) {
	state := control.NewControlState("test")

	_, err := state.CreateSpace("orders", "session")
	if !errors.Is(err, control.ErrNamespaceNotFound) {
		t.Fatalf("err = %v, want ErrNamespaceNotFound", err)
	}
}

func TestControlStateBumpsNamespaceAndSpaceVersions(t *testing.T) {
	state := control.NewControlState("test")
	createNamespaceAndSpace(t, state, "orders", "session")

	namespace, err := state.BumpNamespaceVersion("orders")
	if err != nil {
		t.Fatalf("bump namespace version: %v", err)
	}
	if namespace.Revision != 3 || namespace.Namespace.Version != 2 {
		t.Fatalf("namespace bump = %+v, want revision 3 version 2", namespace)
	}

	space, err := state.BumpSpaceVersion("orders", "session")
	if err != nil {
		t.Fatalf("bump space version: %v", err)
	}
	if space.Revision != 4 || space.Space.Version != 2 {
		t.Fatalf("space bump = %+v, want revision 4 version 2", space)
	}

	snapshot := state.Snapshot()
	if snapshot.Namespaces[0].Version != 2 || snapshot.Spaces[0].Version != 2 {
		t.Fatalf("snapshot versions = namespaces=%+v spaces=%+v", snapshot.Namespaces, snapshot.Spaces)
	}
}

func TestControlStateBumpVersionRequiresExistingCatalogObjects(t *testing.T) {
	state := control.NewControlState("test")

	_, err := state.BumpNamespaceVersion("orders")
	if !errors.Is(err, control.ErrNamespaceNotFound) {
		t.Fatalf("namespace err = %v, want ErrNamespaceNotFound", err)
	}

	if _, createErr := state.CreateNamespace("orders"); createErr != nil {
		t.Fatalf("create namespace: %v", createErr)
	}
	_, err = state.BumpSpaceVersion("orders", "session")
	if !errors.Is(err, control.ErrSpaceNotFound) {
		t.Fatalf("space err = %v, want ErrSpaceNotFound", err)
	}
}

func TestControlStateRejectsInvalidCatalogNames(t *testing.T) {
	state := control.NewControlState("test")
	for _, test := range []struct {
		name            string
		namespace       string
		space           string
		createNamespace bool
		want            error
	}{
		{name: "empty namespace", namespace: "", createNamespace: true, want: control.ErrInvalidNamespace},
		{name: "slash namespace", namespace: "orders/session", createNamespace: true, want: control.ErrInvalidNamespace},
		{name: "empty space", namespace: "orders", space: "", want: control.ErrInvalidSpace},
		{name: "slash space", namespace: "orders", space: "bad/session", want: control.ErrInvalidSpace},
	} {
		t.Run(test.name, func(t *testing.T) {
			var err error
			if test.createNamespace {
				_, err = state.CreateNamespace(test.namespace)
			} else {
				_, err = state.CreateSpace(test.namespace, test.space)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("err = %v, want %v", err, test.want)
			}
		})
	}
}

func TestControlStateSnapshotBuildsSpaceScopedRoutes(t *testing.T) {
	state := control.NewControlState("test")
	registerNode(t, state)
	registerSpecificNode(t, state, "node-2", "127.0.0.1:7503")
	createNamespaceAndSpace(t, state, "orders", "session")

	snapshot := state.Snapshot()
	requireCatalogCounts(t,
		controlapi.NamespacesBody{Namespaces: snapshot.Namespaces},
		controlapi.SpacesBody{Spaces: snapshot.Spaces},
		1,
		1,
	)
	if len(snapshot.Routes) != 2 {
		t.Fatalf("routes len = %d, want 2: %+v", len(snapshot.Routes), snapshot.Routes)
	}
	assertSpaceRoute(t, snapshot.Routes[0], "orders", "session", "node-1", 0, 32767)
	assertSpaceRoute(t, snapshot.Routes[1], "orders", "session", "node-2", 32768, controlapi.VSlotMax)
}

func assertNamespaceResponse(t *testing.T, response controlapi.CreateNamespaceResponse, revision uint64, namespace string) {
	t.Helper()
	if response.Revision != revision || response.Namespace.Namespace != namespace || response.Namespace.Version != 1 {
		t.Fatalf("unexpected namespace response: %+v", response)
	}
}

func assertSpaceResponse(t *testing.T, response controlapi.CreateSpaceResponse, revision uint64, namespace, space string) {
	t.Helper()
	if response.Revision != revision || response.Space.Namespace != namespace || response.Space.Space != space {
		t.Fatalf("unexpected space response: %+v", response)
	}
}

func createNamespaceAndSpace(t *testing.T, state *control.ControlState, namespace, space string) {
	t.Helper()
	if _, err := state.CreateNamespace(namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := state.CreateSpace(namespace, space); err != nil {
		t.Fatalf("create space: %v", err)
	}
}

func requireCatalogCounts(t *testing.T, namespaces controlapi.NamespacesBody, spaces controlapi.SpacesBody, wantNamespaces, wantSpaces int) {
	t.Helper()
	if len(namespaces.Namespaces) != wantNamespaces {
		t.Fatalf("namespaces len = %d, want %d: %+v", len(namespaces.Namespaces), wantNamespaces, namespaces.Namespaces)
	}
	if len(spaces.Spaces) != wantSpaces {
		t.Fatalf("spaces len = %d, want %d: %+v", len(spaces.Spaces), wantSpaces, spaces.Spaces)
	}
}

func assertSpaceRoute(t *testing.T, route controlapi.RouteBody, namespace, space, nodeID string, start, end uint32) {
	t.Helper()
	if route.Namespace != namespace || route.Space != space {
		t.Fatalf("route scope = %s/%s, want %s/%s", route.Namespace, route.Space, namespace, space)
	}
	assertRouteRange(t, route, nodeID, start, end)
}
