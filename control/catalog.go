package control

import (
	"context"
	"sort"

	"github.com/lyonbrown4d/nespa/controlapi"
)

type spaceRef struct {
	namespace string
	space     string
}

type entityRef struct {
	namespace string
	space     string
	entity    string
}

func (s *ControlState) CreateNamespace(namespace string) (controlapi.CreateNamespaceResponse, error) {
	namespace, err := normalizeNamespace(namespace)
	if err != nil {
		return controlapi.CreateNamespaceResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.namespaces.Get(namespace)
	if !exists {
		s.revision++
		item = controlapi.NamespaceBody{
			Namespace:     namespace,
			Version:       1,
			CreatedAtUnix: s.now().Unix(),
		}
		s.namespaces.Set(namespace, item)
	}

	return controlapi.CreateNamespaceResponse{
		Revision:  s.revision,
		Namespace: item,
	}, nil
}

func (s *ControlState) CreateSpace(ctx context.Context, namespace, space string) (controlapi.CreateSpaceResponse, error) {
	namespace, space, err := normalizeSpaceIdentity(namespace, space)
	if err != nil {
		return controlapi.CreateSpaceResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.namespaces.Get(namespace); !exists {
		return controlapi.CreateSpaceResponse{}, namespaceNotFound(namespace)
	}

	ref := spaceRef{namespace: namespace, space: space}
	item, exists := s.spaces.Get(ref)
	if !exists {
		s.revision++
		item = controlapi.SpaceBody{
			Namespace:     namespace,
			Space:         space,
			Version:       1,
			CreatedAtUnix: s.now().Unix(),
		}
		s.spaces.Set(ref, item)
		s.recordRebalanceEventLocked(ctx, rebalanceEvent{
			reason:    rebalanceReasonSpaceCreated,
			namespace: namespace,
			space:     space,
		})
	}

	return controlapi.CreateSpaceResponse{
		Revision: s.revision,
		Space:    item,
	}, nil
}

func (s *ControlState) CreateEntity(namespace, space, entity string) (controlapi.CreateEntityResponse, error) {
	namespace, space, entity, err := normalizeEntityIdentity(namespace, space, entity)
	if err != nil {
		return controlapi.CreateEntityResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.namespaces.Get(namespace); !exists {
		return controlapi.CreateEntityResponse{}, namespaceNotFound(namespace)
	}
	if _, exists := s.spaces.Get(spaceRef{namespace: namespace, space: space}); !exists {
		return controlapi.CreateEntityResponse{}, spaceNotFound(namespace, space)
	}

	ref := entityRef{namespace: namespace, space: space, entity: entity}
	item, exists := s.entities.Get(ref)
	if !exists {
		s.revision++
		item = controlapi.EntityBody{
			Namespace:     namespace,
			Space:         space,
			Entity:        entity,
			CreatedAtUnix: s.now().Unix(),
		}
		s.entities.Set(ref, item)
	}

	return controlapi.CreateEntityResponse{
		Revision: s.revision,
		Entity:   item,
	}, nil
}

func (s *ControlState) BumpNamespaceVersion(namespace string) (controlapi.BumpNamespaceVersionResponse, error) {
	namespace, err := normalizeNamespace(namespace)
	if err != nil {
		return controlapi.BumpNamespaceVersionResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.namespaces.Get(namespace)
	if !exists {
		return controlapi.BumpNamespaceVersionResponse{}, namespaceNotFound(namespace)
	}
	item.Version++
	s.revision++
	s.namespaces.Set(namespace, item)

	return controlapi.BumpNamespaceVersionResponse{
		Revision:  s.revision,
		Namespace: item,
	}, nil
}

func (s *ControlState) BumpSpaceVersion(namespace, space string) (controlapi.BumpSpaceVersionResponse, error) {
	namespace, space, err := normalizeSpaceIdentity(namespace, space)
	if err != nil {
		return controlapi.BumpSpaceVersionResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.namespaces.Get(namespace); !exists {
		return controlapi.BumpSpaceVersionResponse{}, namespaceNotFound(namespace)
	}
	ref := spaceRef{namespace: namespace, space: space}
	item, exists := s.spaces.Get(ref)
	if !exists {
		return controlapi.BumpSpaceVersionResponse{}, spaceNotFound(namespace, space)
	}
	item.Version++
	s.revision++
	s.spaces.Set(ref, item)

	return controlapi.BumpSpaceVersionResponse{
		Revision: s.revision,
		Space:    item,
	}, nil
}

func (s *ControlState) Namespaces() controlapi.NamespacesBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return controlapi.NamespacesBody{
		Revision:   s.revision,
		Namespaces: s.sortedNamespacesLocked(),
	}
}

func (s *ControlState) Spaces() controlapi.SpacesBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return controlapi.SpacesBody{
		Revision: s.revision,
		Spaces:   s.sortedSpacesLocked(),
	}
}

func (s *ControlState) Entities() controlapi.EntitiesBody {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return controlapi.EntitiesBody{
		Revision: s.revision,
		Entities: s.sortedEntitiesLocked(),
	}
}

func (s *ControlState) sortedNamespacesLocked() []controlapi.NamespaceBody {
	namespaces := s.namespaces.Values()
	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].Namespace < namespaces[j].Namespace
	})
	return namespaces
}

func (s *ControlState) sortedSpacesLocked() []controlapi.SpaceBody {
	spaces := s.spaces.Values()
	sort.Slice(spaces, func(i, j int) bool {
		if spaces[i].Namespace == spaces[j].Namespace {
			return spaces[i].Space < spaces[j].Space
		}
		return spaces[i].Namespace < spaces[j].Namespace
	})
	return spaces
}

func (s *ControlState) sortedEntitiesLocked() []controlapi.EntityBody {
	entities := s.entities.Values()
	sort.Slice(entities, func(i, j int) bool {
		switch {
		case entities[i].Namespace != entities[j].Namespace:
			return entities[i].Namespace < entities[j].Namespace
		case entities[i].Space != entities[j].Space:
			return entities[i].Space < entities[j].Space
		default:
			return entities[i].Entity < entities[j].Entity
		}
	})
	return entities
}
