// Package predicted is the workflow of the two PREDICTED topologies of the
// retry-pathology campaign: Single (edge -> svc-A) and Multichain
// (edge-A -> A -> B -> C, edge-X -> X -> Y -> C).
//
// The application does nothing except spend a configured amount of time and
// call its one downstream, because the campaign's claim is about the resilience
// machinery around the call and not about any business logic inside it. Every
// node is a station whose service time is a lognormal draw (CONTRACTS.md §3)
// and whose queue is the interceptor's admission station (PLAN.md D4), so the
// deployed system is the M/G/c/K network the matched simulation instantiates.
package predicted

import (
	"context"

	"github.com/blueprint-uservices/blueprint/runtime/plugins/rpcpolicy"
)

// NodeService is the one internal interface of both predicted topologies. Every
// gRPC edge of every predicted system is therefore /grpc.NodeService/Call,
// which is the method key the policy bundles bind (CONTRACTS.md §7).
//
// `depth` is the hop index: an edge calls its chain head at depth 0 and every
// relay increments it. The leaf returns key + depth, so a response identifies
// the chain that produced it and a smoke probe can tell a Single answer from a
// Multichain one without reading a log.
type NodeService interface {
	Call(ctx context.Context, key int64, depth int64) (int64, error)
}

// LeafNode is a node with no downstream: it burns its service time and returns.
// svc-A on Single and svc-C on Multichain are LeafNodes.
type LeafNode struct {
	service *serviceTime
}

// NewLeafNode builds a leaf from the node config at configPath (overridden by
// NODE_CONFIG_PATH).
func NewLeafNode(ctx context.Context, configPath string) (*LeafNode, error) {
	cfg, err := LoadNodeConfig(configPath)
	if err != nil {
		return nil, err
	}
	return &LeafNode{service: newServiceTime(cfg)}, nil
}

// Call burns the configured service time and returns.
func (n *LeafNode) Call(ctx context.Context, key int64, depth int64) (int64, error) {
	if err := n.service.burn(ctx); err != nil {
		return 0, err
	}
	return key + depth, nil
}

// RelayNode is a node with exactly one downstream (fanout: serial).
type RelayNode struct {
	downstream NodeService
	service    *serviceTime
}

// NewRelayNode builds a relay over `downstream` from the node config at
// configPath (overridden by NODE_CONFIG_PATH).
func NewRelayNode(ctx context.Context, downstream NodeService, configPath string) (*RelayNode, error) {
	cfg, err := LoadNodeConfig(configPath)
	if err != nil {
		return nil, err
	}
	return &RelayNode{downstream: downstream, service: newServiceTime(cfg)}, nil
}

// Call burns this node's own service time, hands the admission permit back, and
// then calls downstream.
//
// The ReleasePermit between the two is msim's hold_worker_during_fanout=False,
// the default: a relay waiting on its downstream is not occupying one of its
// own workers, so the relay's station models its own service and not the whole
// subtree. Ablation A4 (`hold_permit_through_fanout: true` in the policy YAML)
// turns the call into a no-op and gets the other semantics without a recompile.
func (n *RelayNode) Call(ctx context.Context, key int64, depth int64) (int64, error) {
	if err := n.service.burn(ctx); err != nil {
		return 0, err
	}
	rpcpolicy.ReleasePermit(ctx)
	return n.downstream.Call(ctx, key, depth+1)
}
