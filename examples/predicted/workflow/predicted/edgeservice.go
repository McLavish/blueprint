package predicted

import (
	"context"
)

// EdgeService is the front door of a predicted topology. It is deployed over
// HTTP, so the driver reaches it as
//
//	GET /Root?key=<int64>   ->   {"Ret0": <int64>}
//
// and the interceptor's HTTP middleware writes the ROOT attempt-log record that
// the pipeline joins to the driver's client.csv by trace id (CONTRACTS.md §5).
type EdgeService interface {
	Root(ctx context.Context, key int64) (int64, error)
}

// EdgeNode is the front-door implementation. It has NO service time of its own:
// the edge exists to own a client-side policy (its container is the one that
// retries), and giving it a station too would put a second queue in front of
// the one the experiment is about.
type EdgeNode struct {
	downstream NodeService
}

// NewEdgeNode builds the front door over `downstream`.
func NewEdgeNode(ctx context.Context, downstream NodeService) (*EdgeNode, error) {
	return &EdgeNode{downstream: downstream}, nil
}

// Root calls the chain head at depth 0.
func (e *EdgeNode) Root(ctx context.Context, key int64) (int64, error) {
	return e.downstream.Call(ctx, key, 0)
}
