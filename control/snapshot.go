package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/nespa/controlapi"
)

type Snapshot struct {
	ClusterID  string                          `json:"cluster_id"`
	Revision   uint64                          `json:"revision"`
	Mode       string                          `json:"mode"`
	Namespaces []controlapi.NamespaceBody      `json:"namespaces,omitempty"`
	Spaces     []controlapi.SpaceBody          `json:"spaces,omitempty"`
	Entities   []controlapi.EntityBody         `json:"entities,omitempty"`
	Nodes      []controlapi.NodeBody           `json:"nodes,omitempty"`
	Events     []controlapi.RebalanceEventBody `json:"events,omitempty"`
	Plans      []controlapi.MigrationPlanBody  `json:"plans,omitempty"`
	NextEvent  uint64                          `json:"next_event,omitempty"`
	NextPlan   uint64                          `json:"next_plan,omitempty"`
}

func (s *ControlState) ExportSnapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return Snapshot{
		ClusterID:  s.clusterID,
		Revision:   s.revision,
		Mode:       controlModeBootstrap,
		Namespaces: s.sortedNamespacesLocked(),
		Spaces:     s.sortedSpacesLocked(),
		Entities:   s.sortedEntitiesLocked(),
		Nodes:      s.sortedNodesLocked(),
		Events:     s.events.Values(),
		Plans:      s.plans.Values(),
		NextEvent:  s.nextEvent,
		NextPlan:   s.nextPlan,
	}
}

func (s *ControlState) RestoreSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	namespaces := collectionmapping.NewMap[string, controlapi.NamespaceBody]()
	for index := range snapshot.Namespaces {
		item := snapshot.Namespaces[index]
		namespace, err := normalizeNamespace(item.Namespace)
		if err != nil {
			return fmt.Errorf("restore namespace: %w", err)
		}
		namespaces.Set(namespace, item)
	}

	spaces, err := restoreSpaces(snapshot.Spaces)
	if err != nil {
		return err
	}
	entities, err := restoreEntities(snapshot.Entities)
	if err != nil {
		return err
	}
	nodes, err := restoreNodes(snapshot.Nodes)
	if err != nil {
		return err
	}

	s.clusterID = snapshot.ClusterID
	s.revision = snapshot.Revision
	s.namespaces = namespaces
	s.spaces = spaces
	s.entities = entities
	s.nodes = nodes
	s.events = collectionlist.NewList[controlapi.RebalanceEventBody](snapshot.Events...)
	s.plans = collectionlist.NewList[controlapi.MigrationPlanBody](snapshot.Plans...)
	s.nextEvent = maxEventID(snapshot.Events, snapshot.NextEvent)
	s.nextPlan = maxPlanID(snapshot.Plans, snapshot.NextPlan)
	s.lastRoutes = routesForNodes(s.sortedNodesLocked(), s.sortedSpacesLocked())
	return nil
}

func LoadSnapshotFile(path string) (Snapshot, error) {
	dir, name := snapshotDirAndName(path)
	raw, err := fs.ReadFile(os.DirFS(dir), name)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read control snapshot: %w", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("decode control snapshot: %w", err)
	}
	return snapshot, nil
}

func SaveSnapshotFile(path string, snapshot Snapshot) error {
	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode control snapshot: %w", err)
	}
	dir, name := snapshotDirAndName(path)
	if mkdirErr := os.MkdirAll(dir, 0o750); mkdirErr != nil {
		return fmt.Errorf("create control snapshot dir: %w", mkdirErr)
	}
	tmp, err := os.CreateTemp(dir, "."+name+".*.tmp")
	if err != nil {
		return fmt.Errorf("create control snapshot temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, writeErr := tmp.Write(raw); writeErr != nil {
		return errors.Join(
			fmt.Errorf("write control snapshot temp file: %w", writeErr),
			closeSnapshotTemp(tmp),
			removeSnapshotTemp(tmpName),
		)
	}
	if err := tmp.Close(); err != nil {
		return errors.Join(
			fmt.Errorf("close control snapshot temp file: %w", err),
			removeSnapshotTemp(tmpName),
		)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return errors.Join(
			fmt.Errorf("write control snapshot: %w", err),
			removeSnapshotTemp(tmpName),
		)
	}
	return nil
}

func closeSnapshotTemp(file *os.File) error {
	if err := file.Close(); err != nil {
		return fmt.Errorf("close control snapshot temp file: %w", err)
	}
	return nil
}

func removeSnapshotTemp(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove control snapshot temp file: %w", err)
	}
	return nil
}

func snapshotDirAndName(path string) (string, string) {
	clean := filepath.Clean(path)
	dir, name := filepath.Split(clean)
	if dir == "" {
		dir = "."
	}
	return dir, name
}

func restoreSpaces(items []controlapi.SpaceBody) (*collectionmapping.Map[spaceRef, controlapi.SpaceBody], error) {
	spaces := collectionmapping.NewMap[spaceRef, controlapi.SpaceBody]()
	for index := range items {
		item := items[index]
		namespace, space, err := normalizeSpaceIdentity(item.Namespace, item.Space)
		if err != nil {
			return nil, fmt.Errorf("restore space: %w", err)
		}
		spaces.Set(spaceRef{namespace: namespace, space: space}, item)
	}
	return spaces, nil
}

func restoreEntities(items []controlapi.EntityBody) (*collectionmapping.Map[entityRef, controlapi.EntityBody], error) {
	entities := collectionmapping.NewMap[entityRef, controlapi.EntityBody]()
	for index := range items {
		item := items[index]
		namespace, space, entity, err := normalizeEntityIdentity(item.Namespace, item.Space, item.Entity)
		if err != nil {
			return nil, fmt.Errorf("restore entity: %w", err)
		}
		entities.Set(entityRef{namespace: namespace, space: space, entity: entity}, item)
	}
	return entities, nil
}

func restoreNodes(items []controlapi.NodeBody) (*collectionmapping.Map[string, controlapi.NodeBody], error) {
	nodes := collectionmapping.NewMap[string, controlapi.NodeBody]()
	for index := range items {
		item := items[index]
		nodeID, _, err := validateNodeIdentity(item.NodeID, item.Addr)
		if err != nil {
			return nil, fmt.Errorf("restore node: %w", err)
		}
		nodes.Set(nodeID, item)
	}
	return nodes, nil
}

func maxEventID(items []controlapi.RebalanceEventBody, current uint64) uint64 {
	for index := range items {
		if items[index].ID > current {
			current = items[index].ID
		}
	}
	return current
}

func maxPlanID(items []controlapi.MigrationPlanBody, current uint64) uint64 {
	for index := range items {
		if items[index].ID > current {
			current = items[index].ID
		}
	}
	return current
}
