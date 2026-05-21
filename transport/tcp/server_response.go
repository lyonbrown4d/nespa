package tcp

import (
	"github.com/lyonbrown4d/nespa/cache"
	"github.com/lyonbrown4d/nespa/cachewire"
	"github.com/lyonbrown4d/nespa/protocol"
)

func recordFrame(frame protocol.Frame, rec cache.Record, found bool) protocol.Frame {
	if !found {
		return metadataFrame(frame, cachewire.EncodeRecord(cachewire.Record{Found: false}), nil)
	}
	body := recordFromCache(rec, true)
	return metadataFrame(frame, cachewire.EncodeRecord(body), rec.Value)
}
