package specs

import (
	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/examples/predicted/workflow/predicted"
	"github.com/blueprint-uservices/blueprint/plugins/cmdbuilder"
	"github.com/blueprint-uservices/blueprint/plugins/workflow"
)

// Single is the smallest predicted topology: an HTTP front door in front of one
// station.
//
//	edge (HTTP /Root) --grpc--> svc_a (LeafNode)
//
// It carries runs K4-K7: calibration, the three-fault figure, and the whole
// metastability sweep. One retrying client, one queue, one service time --
// every number the campaign reports about pathology 3 on this system is a
// property of that single pair.
var Single = cmdbuilder.SpecOption{
	Name:        "single",
	Description: "Predicted topology Single: an HTTP edge in front of one gRPC leaf node (svc-A).",
	Build:       makeSingleSpec,
}

func makeSingleSpec(spec wiring.WiringSpec) ([]string, error) {
	svc_a := workflow.Service[*predicted.LeafNode](spec, "svc_a", NodeConfigPath)
	svc_a_ctr := grpcContainer(spec, svc_a)

	edge := workflow.Service[*predicted.EdgeNode](spec, "edge", svc_a)
	edge_ctr := httpContainer(spec, edge)

	return []string{svc_a_ctr, edge_ctr}, nil
}
