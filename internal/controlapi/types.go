package controlapi

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

type RouteBody struct {
	Namespace string `json:"namespace,omitempty"`
	Space     string `json:"space,omitempty"`
	NodeID    string `json:"node_id"`
	Addr      string `json:"addr"`
	Weight    uint32 `json:"weight"`
}

type SnapshotBody struct {
	ClusterID string      `json:"cluster_id"`
	Revision  uint64      `json:"revision"`
	Mode      string      `json:"mode"`
	Nodes     []NodeBody  `json:"nodes"`
	Routes    []RouteBody `json:"routes"`
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
