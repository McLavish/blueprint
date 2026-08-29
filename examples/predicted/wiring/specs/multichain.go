package specs

import (
	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/examples/predicted/workflow/predicted"
	"github.com/blueprint-uservices/blueprint/plugins/cmdbuilder"
	"github.com/blueprint-uservices/blueprint/plugins/workflow"
)

// Multichain is two three-hop chains that share their leaf:
//
//	edge_a (HTTP /Root) --> svc_a --> svc_b --> svc_c
//	edge_x (HTTP /Root) --> svc_x --> svc_y --> svc_c
//
// svc_c is the shared service and the only faulted one. The A-chain is the
// chain under test and the X-chain is the bystander: run K9 measures how a
// per-edge retry allowance multiplies into load at svc_c, and run K10 measures
// what that does to the X-chain, which never retried anything.
//
// Every internal edge is /grpc.NodeService/Call, so a per-edge policy is the
// calling container's own bundle and nothing else (CONTRACTS.md §7).
var Multichain = cmdbuilder.SpecOption{
	Name:        "multichain",
	Description: "Predicted topology Multichain: two three-hop chains (A->B->C, X->Y->C) sharing leaf svc-C.",
	Build:       makeMultichainSpec,
}

func makeMultichainSpec(spec wiring.WiringSpec) ([]string, error) {
	// The shared leaf first: both chains name it as their downstream.
	svc_c := workflow.Service[*predicted.LeafNode](spec, "svc_c", NodeConfigPath)
	svc_c_ctr := grpcContainer(spec, svc_c)

	// A-chain.
	svc_b := workflow.Service[*predicted.RelayNode](spec, "svc_b", svc_c, NodeConfigPath)
	svc_b_ctr := grpcContainer(spec, svc_b)

	svc_a := workflow.Service[*predicted.RelayNode](spec, "svc_a", svc_b, NodeConfigPath)
	svc_a_ctr := grpcContainer(spec, svc_a)

	edge_a := workflow.Service[*predicted.EdgeNode](spec, "edge_a", svc_a)
	edge_a_ctr := httpContainer(spec, edge_a)

	// X-chain (the bystander).
	svc_y := workflow.Service[*predicted.RelayNode](spec, "svc_y", svc_c, NodeConfigPath)
	svc_y_ctr := grpcContainer(spec, svc_y)

	svc_x := workflow.Service[*predicted.RelayNode](spec, "svc_x", svc_y, NodeConfigPath)
	svc_x_ctr := grpcContainer(spec, svc_x)

	edge_x := workflow.Service[*predicted.EdgeNode](spec, "edge_x", svc_x)
	edge_x_ctr := httpContainer(spec, edge_x)

	return []string{
		svc_c_ctr,
		svc_b_ctr, svc_a_ctr, edge_a_ctr,
		svc_y_ctr, svc_x_ctr, edge_x_ctr,
	}, nil
}
