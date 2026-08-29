// Package specs contains the wiring specs of the two PREDICTED topologies of
// the retry-pathology campaign.
//
// The two specs are deliberately spare. There is no opentelemetry.Instrument
// (the attempt log is the interceptor's own JSONL file, PLAN.md D6, so there is
// no Jaeger collector to point a tracer at), no workload.Generator and no
// gotests.Test (the load comes from the harness's open-loop driver on its own
// VMs, PLAN.md D7), and no clientpool (a pool of clients in front of the
// interceptor would put an unmodelled queue between the retry loop and the
// wire). What is left is exactly the structure the matched simulation
// instantiates: nodes, one gRPC edge between each pair, and an HTTP front door.
package specs

import (
	"fmt"

	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/plugins/goproc"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
	"github.com/blueprint-uservices/blueprint/plugins/http"
	"github.com/blueprint-uservices/blueprint/plugins/linuxcontainer"
)

// NodeConfigPath is the node config path baked into every node constructor.
// The container mounts its own node.yaml there read-only; NODE_CONFIG_PATH
// overrides it (CONTRACTS.md §1, §3), which is what lets a sweep arm change the
// service time with a container restart instead of a recompile.
const NodeConfigPath = "/opt/app/conf/predicted/node.yaml"

// grpcContainer deploys one internal service: gRPC over the wire, one process,
// one container. The process and container names are `<svc>_process` and
// `<svc>_container`, which is the contract benchmarks/<system>/placement.yaml
// and the compose splitter both read (CONTRACTS.md §7).
func grpcContainer(spec wiring.WiringSpec, svc string) string {
	grpc.Deploy(spec, svc)
	return processAndContainer(spec, svc)
}

// httpContainer deploys a front door: HTTP over the wire, one process, one
// container.
func httpContainer(spec wiring.WiringSpec, svc string) string {
	http.Deploy(spec, svc)
	return processAndContainer(spec, svc)
}

func processAndContainer(spec wiring.WiringSpec, svc string) string {
	procName := fmt.Sprintf("%s_process", svc)
	ctrName := fmt.Sprintf("%s_container", svc)
	goproc.CreateProcess(spec, procName, svc)
	return linuxcontainer.CreateContainer(spec, ctrName, procName)
}
