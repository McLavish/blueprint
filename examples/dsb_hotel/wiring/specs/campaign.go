package specs

import (
	"fmt"

	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/examples/dsb_hotel/workflow/hotelreservation"
	"github.com/blueprint-uservices/blueprint/plugins/cmdbuilder"
	"github.com/blueprint-uservices/blueprint/plugins/goproc"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
	"github.com/blueprint-uservices/blueprint/plugins/http"
	"github.com/blueprint-uservices/blueprint/plugins/linuxcontainer"
	"github.com/blueprint-uservices/blueprint/plugins/memcached"
	"github.com/blueprint-uservices/blueprint/plugins/mongodb"
	"github.com/blueprint-uservices/blueprint/plugins/workflow"
)

// Campaign is `original` with everything the retry-pathology campaign does not
// deploy taken out, and nothing else changed. Same eight services, same six
// MongoDB and three memcached containers, same container/process names
// (`<svc>_container`, `<svc>_process`, `<backend>_ctr`), same gRPC edges and the
// same HTTP front door.
//
// What is gone and why (PLAN.md §3.4, D6, D7):
//
//   - `jaeger.Collector` and `opentelemetry.Instrument`: the attempt log is the
//     interceptor's own JSONL file, so there is no collector to point a tracer
//     at. The instrumentation is not merely dead weight here -- the otel client
//     wrapper appends a `traceCtx` argument to every RPC and rewrites the
//     generated proto service names, so leaving it in would change the very
//     gRPC full-method strings (`/grpc.<Iface>/<Method>`) that the policy YAML's
//     `method_policies` keys and the attempt log's `operation` field are keyed
//     by (CONTRACTS.md §2, §5).
//   - `workload.Generator[workloadgen.ComplexWorkload]` (`cmplx_workload`): the
//     load is offered open-loop by the harness's own driver on its own VMs, and
//     a closed-loop generator inside the deployment would add unmodelled
//     arrivals the matched simulation does not have.
//   - `gotests.Test`: the generated test container is not deployed and pulls the
//     `tests` module into the build for nothing.
//
// Driven paths are `/SearchHandler` and `/RecommendHandler`; the faulted /
// shared service is `profile_service` (PLAN.md §7).
var Campaign = cmdbuilder.SpecOption{
	Name:        "campaign",
	Description: "Campaign configuration: `original` without jaeger/opentelemetry, the workload generator and the tests.",
	Build:       makeCampaignSpec,
}

func makeCampaignSpec(spec wiring.WiringSpec) ([]string, error) {
	var cntrs []string

	// Define backends
	user_db := mongodb.Container(spec, "user_db")
	recommendations_db := mongodb.Container(spec, "recomd_db")
	reserv_db := mongodb.Container(spec, "reserv_db")
	geo_db := mongodb.Container(spec, "geo_db")
	rate_db := mongodb.Container(spec, "rate_db")
	profile_db := mongodb.Container(spec, "profile_db")

	reserv_cache := memcached.Container(spec, "reserv_cache")
	rate_cache := memcached.Container(spec, "rate_cache")
	profile_cache := memcached.Container(spec, "profile_cache")

	// Define internal services
	user_service := workflow.Service[hotelreservation.UserService](spec, "user_service", user_db)
	cntrs = append(cntrs, applyCampaignDefaults(spec, user_service))

	recomd_service := workflow.Service[hotelreservation.RecommendationService](spec, "recomd_service", recommendations_db)
	cntrs = append(cntrs, applyCampaignDefaults(spec, recomd_service))

	reserv_service := workflow.Service[hotelreservation.ReservationService](spec, "reserv_service", reserv_cache, reserv_db)
	cntrs = append(cntrs, applyCampaignDefaults(spec, reserv_service))

	geo_service := workflow.Service[hotelreservation.GeoService](spec, "geo_service", geo_db)
	cntrs = append(cntrs, applyCampaignDefaults(spec, geo_service))

	rate_service := workflow.Service[hotelreservation.RateService](spec, "rate_service", rate_cache, rate_db)
	cntrs = append(cntrs, applyCampaignDefaults(spec, rate_service))

	profile_service := workflow.Service[hotelreservation.ProfileService](spec, "profile_service", profile_cache, profile_db)
	cntrs = append(cntrs, applyCampaignDefaults(spec, profile_service))

	search_service := workflow.Service[hotelreservation.SearchService](spec, "search_service", geo_service, rate_service)
	cntrs = append(cntrs, applyCampaignDefaults(spec, search_service))

	// Define frontend service
	frontend_service := workflow.Service[hotelreservation.FrontEndService](spec, "frontend_service", search_service, profile_service, recomd_service, user_service, reserv_service)
	cntrs = append(cntrs, applyCampaignHTTPDefaults(spec, frontend_service))

	return cntrs, nil
}

// applyCampaignDefaults is `applyDefaults` without the opentelemetry wrapper.
// The process and container names are the ones `original` emits, verbatim.
func applyCampaignDefaults(spec wiring.WiringSpec, serviceName string) string {
	procName := fmt.Sprintf("%s_process", serviceName)
	ctrName := fmt.Sprintf("%s_container", serviceName)
	grpc.Deploy(spec, serviceName)
	goproc.CreateProcess(spec, procName, serviceName)
	return linuxcontainer.CreateContainer(spec, ctrName, procName)
}

// applyCampaignHTTPDefaults is `applyHTTPDefaults` without the opentelemetry
// wrapper: the front door the harness's driver aims at.
func applyCampaignHTTPDefaults(spec wiring.WiringSpec, serviceName string) string {
	procName := fmt.Sprintf("%s_process", serviceName)
	ctrName := fmt.Sprintf("%s_container", serviceName)
	http.Deploy(spec, serviceName)
	goproc.CreateProcess(spec, procName, serviceName)
	return linuxcontainer.CreateContainer(spec, ctrName, procName)
}
