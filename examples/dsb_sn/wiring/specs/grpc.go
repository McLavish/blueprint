package specs

import (
	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/examples/dsb_sn/workflow/socialnetwork"
	"github.com/blueprint-uservices/blueprint/plugins/cmdbuilder"
	"github.com/blueprint-uservices/blueprint/plugins/goproc"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
	"github.com/blueprint-uservices/blueprint/plugins/http"
	"github.com/blueprint-uservices/blueprint/plugins/linuxcontainer"
	"github.com/blueprint-uservices/blueprint/plugins/memcached"
	"github.com/blueprint-uservices/blueprint/plugins/mongodb"
	"github.com/blueprint-uservices/blueprint/plugins/workflow"
)

// Grpc is `docker` with the twelve internal services re-wired from Thrift to
// gRPC, and without the generated test container.
//
// The re-wire is what makes socialNetwork measurable by the campaign at all:
// the `rpcpolicy` interceptor is a gRPC unary interceptor pair
// (`plugins/grpc/grpccodegen`), so a Thrift edge carries no retry policy, no
// admission station, no fault injector and no attempt-log record. The frontend
// (`wrk2api_service`) stays HTTP -- it is the driven front door.
//
// Everything else is `docker` verbatim: the same thirteen services, the same
// five MongoDB and five memcached containers, and the same process/container
// names (including upstream's `socailgraph_proc` typo, which is a name the
// placement files and the compose splitter read).
//
// The faulted / shared service is `post_storage_service` (PLAN.md §7); the
// driven paths are `/ComposePost` and `/ReadHomeTimeline`, with `/Register` and
// `/Follow` used by the data loader (PLAN.md D21).
var Grpc = cmdbuilder.SpecOption{
	Name:        "grpc",
	Description: "Deploys each service in a separate container with grpc, and uses mongodb as NoSQL database backends.",
	Build:       makeGrpcSpec,
}

func makeGrpcSpec(spec wiring.WiringSpec) ([]string, error) {
	var containers []string

	// Define backends
	user_cache := memcached.Container(spec, "user_cache")
	user_db := mongodb.Container(spec, "user_db")
	post_cache := memcached.Container(spec, "post_cache")
	post_db := mongodb.Container(spec, "post_db")
	social_cache := memcached.Container(spec, "social_cache")
	social_db := mongodb.Container(spec, "social_db")
	urlshorten_db := mongodb.Container(spec, "urlshorten_db")
	usertimeline_cache := memcached.Container(spec, "usertimeline_cache")
	usertimeline_db := mongodb.Container(spec, "usertimeline_db")
	hometimeline_cache := memcached.Container(spec, "hometimeline_cache")

	// Define url_shorten service
	urlshorten_service := workflow.Service[socialnetwork.UrlShortenService](spec, "urlshorten_service", urlshorten_db)
	containers = append(containers, applyGrpcDefaults(spec, urlshorten_service, "urlshorten_proc", "urlshorten_container"))

	// Define user_mention service
	usermention_service := workflow.Service[socialnetwork.UserMentionService](spec, "usermention_service", user_cache, user_db)
	containers = append(containers, applyGrpcDefaults(spec, usermention_service, "usermention_proc", "usermention_container"))

	// Define post_storage service
	post_storage_service := workflow.Service[socialnetwork.PostStorageService](spec, "post_storage_service", post_cache, post_db)
	containers = append(containers, applyGrpcDefaults(spec, post_storage_service, "post_storage_proc", "post_storage_container"))

	// Define media service
	media_service := workflow.Service[socialnetwork.MediaService](spec, "media_service")
	containers = append(containers, applyGrpcDefaults(spec, media_service, "media_proc", "media_container"))

	// Define uniqueid service
	uniqueId_service := workflow.Service[socialnetwork.UniqueIdService](spec, "uniqueid_service")
	containers = append(containers, applyGrpcDefaults(spec, uniqueId_service, "uniqueid_proc", "uniqueid_container"))

	// Define user_id service
	userid_service := workflow.Service[socialnetwork.UserIDService](spec, "userid_service", user_cache, user_db)
	containers = append(containers, applyGrpcDefaults(spec, userid_service, "userid_proc", "userid_container"))

	// Define social_graph service
	socialgraph_service := workflow.Service[socialnetwork.SocialGraphService](spec, "socialgraph_service", social_cache, social_db, userid_service)
	containers = append(containers, applyGrpcDefaults(spec, socialgraph_service, "socailgraph_proc", "socialgraph_container"))

	// Define home_timeline service
	hometimeline_service := workflow.Service[socialnetwork.HomeTimelineService](spec, "hometimeline_service", hometimeline_cache, post_storage_service, socialgraph_service)
	containers = append(containers, applyGrpcDefaults(spec, hometimeline_service, "hometimeline_proc", "hometimeline_container"))

	// Define user service
	user_service := workflow.Service[socialnetwork.UserService](spec, "user_service", user_cache, user_db, socialgraph_service, "secret")
	containers = append(containers, applyGrpcDefaults(spec, user_service, "user_proc", "user_container"))

	// Define text service
	text_service := workflow.Service[socialnetwork.TextService](spec, "text_service", urlshorten_service, usermention_service)
	containers = append(containers, applyGrpcDefaults(spec, text_service, "text_proc", "text_container"))

	// Define user_timeline service
	usertimeline_service := workflow.Service[socialnetwork.UserTimelineService](spec, "usertimeline_service", usertimeline_cache, usertimeline_db, post_storage_service)
	containers = append(containers, applyGrpcDefaults(spec, usertimeline_service, "usertimeline_proc", "usertimeline_container"))

	// Define compose post service
	composepost_service := workflow.Service[socialnetwork.ComposePostService](spec, "composepost_service", post_storage_service, usertimeline_service, user_service, uniqueId_service, media_service, text_service, hometimeline_service)
	containers = append(containers, applyGrpcDefaults(spec, composepost_service, "compose_proc", "compose_container"))

	// Define frontend service
	wrk2api_service := workflow.Service[socialnetwork.Wrk2APIService](spec, "wrk2api_service", user_service, composepost_service, usertimeline_service, hometimeline_service, socialgraph_service)
	containers = append(containers, applyGrpcHTTPDefaults(spec, wrk2api_service, "wrk2api_proc", "wrk2api_container"))

	return containers, nil
}

// applyGrpcDefaults is `applyDockerDefaults` with grpc.Deploy in place of
// thrift.Deploy; the process and container names are unchanged.
func applyGrpcDefaults(spec wiring.WiringSpec, serviceName, procName, ctrName string) string {
	grpc.Deploy(spec, serviceName)
	goproc.CreateProcess(spec, procName, serviceName)
	return linuxcontainer.CreateContainer(spec, ctrName, procName)
}

func applyGrpcHTTPDefaults(spec wiring.WiringSpec, serviceName, procName, ctrName string) string {
	http.Deploy(spec, serviceName)
	goproc.CreateProcess(spec, procName, serviceName)
	return linuxcontainer.CreateContainer(spec, ctrName, procName)
}
