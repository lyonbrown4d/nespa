package cacheapi

type SetInput struct {
	Body SetBody
}

type SetBody struct {
	Namespace        string `json:"namespace"`
	Space            string `json:"space"`
	Entity           string `json:"entity,omitempty"`
	Key              string `json:"key"`
	Value            string `json:"value"`
	TTLMillis        int64  `json:"ttl_ms,omitempty"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
}

type GetInput struct {
	Namespace        string `query:"namespace"`
	Space            string `query:"space"`
	Entity           string `query:"entity"`
	Key              string `query:"key"`
	NamespaceVersion uint64 `query:"namespace_version"`
	SpaceVersion     uint64 `query:"space_version"`
}

type DeleteInput struct {
	Namespace string `query:"namespace"`
	Space     string `query:"space"`
	Entity    string `query:"entity"`
	Key       string `query:"key"`
}

type RecordBody struct {
	Found            bool   `json:"found"`
	Namespace        string `json:"namespace,omitempty"`
	Space            string `json:"space,omitempty"`
	Entity           string `json:"entity,omitempty"`
	Key              string `json:"key,omitempty"`
	Value            string `json:"value,omitempty"`
	Version          uint64 `json:"version,omitempty"`
	NamespaceVersion uint64 `json:"namespace_version,omitempty"`
	SpaceVersion     uint64 `json:"space_version,omitempty"`
	ExpireAtUnixMs   int64  `json:"expire_at_ms,omitempty"`
}

type DeleteBody struct {
	Deleted bool `json:"deleted"`
}
