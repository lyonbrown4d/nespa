// Package controlapi defines shared control-plane API DTOs.
package controlapi

const (
	VSlotCount uint32 = 65536
	VSlotMax   uint32 = VSlotCount - 1
)

type StateBody struct {
	ClusterID string `json:"cluster_id"`
	Revision  uint64 `json:"revision"`
	Mode      string `json:"mode"`
}

type NodeBody struct {
	NodeID       string `json:"node_id"`
	Addr         string `json:"addr"`
	State        string `json:"state"`
	LastSeenUnix int64  `json:"last_seen_unix"`
}

type NamespaceBody struct {
	Namespace     string `json:"namespace"`
	Version       uint64 `json:"version"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type SpaceBody struct {
	Namespace     string `json:"namespace"`
	Space         string `json:"space"`
	Version       uint64 `json:"version"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type EntityBody struct {
	Namespace     string `json:"namespace"`
	Space         string `json:"space"`
	Entity        string `json:"entity"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type RouteBody struct {
	Namespace  string `json:"namespace,omitempty"`
	Space      string `json:"space,omitempty"`
	VSlotStart uint32 `json:"vslot_start"`
	VSlotEnd   uint32 `json:"vslot_end"`
	NodeID     string `json:"node_id"`
	Addr       string `json:"addr"`
	Weight     uint32 `json:"weight"`
}

type SnapshotBody struct {
	ClusterID  string          `json:"cluster_id"`
	Revision   uint64          `json:"revision"`
	Mode       string          `json:"mode"`
	Namespaces []NamespaceBody `json:"namespaces,omitempty"`
	Spaces     []SpaceBody     `json:"spaces,omitempty"`
	Entities   []EntityBody    `json:"entities,omitempty"`
	Nodes      []NodeBody      `json:"nodes"`
	Routes     []RouteBody     `json:"routes"`
}

type RebalanceEventBody struct {
	ID            uint64 `json:"id"`
	Revision      uint64 `json:"revision"`
	Type          string `json:"type"`
	Reason        string `json:"reason"`
	NodeID        string `json:"node_id,omitempty"`
	Addr          string `json:"addr,omitempty"`
	State         string `json:"state,omitempty"`
	Namespace     string `json:"namespace,omitempty"`
	Space         string `json:"space,omitempty"`
	RouteCount    int    `json:"route_count"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type RebalanceEventsBody struct {
	Revision uint64               `json:"revision"`
	Events   []RebalanceEventBody `json:"events"`
}

type MigrationTaskBody struct {
	PlanID          uint64 `json:"plan_id"`
	TaskID          uint64 `json:"task_id"`
	Revision        uint64 `json:"revision"`
	Namespace       string `json:"namespace,omitempty"`
	Space           string `json:"space,omitempty"`
	VSlotStart      uint32 `json:"vslot_start"`
	VSlotEnd        uint32 `json:"vslot_end"`
	SourceNodeID    string `json:"source_node_id"`
	SourceAddr      string `json:"source_addr"`
	TargetNodeID    string `json:"target_node_id"`
	TargetAddr      string `json:"target_addr"`
	State           string `json:"state"`
	CreatedAtUnix   int64  `json:"created_at_unix"`
	StartedAtUnix   int64  `json:"started_at_unix,omitempty"`
	FinishedAtUnix  int64  `json:"finished_at_unix,omitempty"`
	ImportedEntries uint64 `json:"imported_entries,omitempty"`
	DeletedEntries  uint64 `json:"deleted_entries,omitempty"`
	Error           string `json:"error,omitempty"`
}

type MigrationPlanBody struct {
	ID            uint64              `json:"id"`
	Revision      uint64              `json:"revision"`
	Reason        string              `json:"reason"`
	State         string              `json:"state"`
	CreatedAtUnix int64               `json:"created_at_unix"`
	Tasks         []MigrationTaskBody `json:"tasks"`
}

type MigrationPlansBody struct {
	Revision uint64              `json:"revision"`
	Plans    []MigrationPlanBody `json:"plans"`
}

type NodesBody struct {
	Revision uint64     `json:"revision"`
	Nodes    []NodeBody `json:"nodes"`
}

type NamespacesBody struct {
	Revision   uint64          `json:"revision"`
	Namespaces []NamespaceBody `json:"namespaces"`
}

type SpacesBody struct {
	Revision uint64      `json:"revision"`
	Spaces   []SpaceBody `json:"spaces"`
}

type EntitiesBody struct {
	Revision uint64       `json:"revision"`
	Entities []EntityBody `json:"entities"`
}

type CreateNamespaceInput struct {
	Body CreateNamespaceBody
}

type CreateNamespaceBody struct {
	Namespace string `json:"namespace"`
}

type CreateNamespaceResponse struct {
	Revision  uint64        `json:"revision"`
	Namespace NamespaceBody `json:"namespace"`
}

type BumpNamespaceVersionInput struct {
	Body BumpNamespaceVersionBody
}

type BumpNamespaceVersionBody struct {
	Namespace string `json:"namespace"`
}

type BumpNamespaceVersionResponse struct {
	Revision  uint64        `json:"revision"`
	Namespace NamespaceBody `json:"namespace"`
}

type CreateSpaceInput struct {
	Body CreateSpaceBody
}

type CreateSpaceBody struct {
	Namespace string `json:"namespace"`
	Space     string `json:"space"`
}

type CreateSpaceResponse struct {
	Revision uint64    `json:"revision"`
	Space    SpaceBody `json:"space"`
}

type BumpSpaceVersionInput struct {
	Body BumpSpaceVersionBody
}

type BumpSpaceVersionBody struct {
	Namespace string `json:"namespace"`
	Space     string `json:"space"`
}

type BumpSpaceVersionResponse struct {
	Revision uint64    `json:"revision"`
	Space    SpaceBody `json:"space"`
}

type CreateEntityInput struct {
	Body CreateEntityBody
}

type CreateEntityBody struct {
	Namespace string `json:"namespace"`
	Space     string `json:"space"`
	Entity    string `json:"entity"`
}

type CreateEntityResponse struct {
	Revision uint64     `json:"revision"`
	Entity   EntityBody `json:"entity"`
}

type RegisterNodeInput struct {
	Body RegisterNodeBody
}

type RegisterNodeBody struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

type RegisterNodeResponse struct {
	Revision uint64   `json:"revision"`
	Node     NodeBody `json:"node"`
}

type HeartbeatInput struct {
	Body HeartbeatBody
}

type HeartbeatBody struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

type HeartbeatResponse struct {
	Revision uint64   `json:"revision"`
	Node     NodeBody `json:"node"`
}
