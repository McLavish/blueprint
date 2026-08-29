# `examples/predicted` — the two predicted topologies

The workflow and wiring specs of the retry-pathology campaign's two *predicted*
systems (`docs/BATCH1-PLAN.md` WP2, `docs/CONTRACTS.md` §7 in the
`microservices-system` repo):

| spec | shape | containers |
|---|---|---|
| `single` | `edge` (HTTP `/Root`) → `svc_a` | `edge_container`, `svc_a_container` |
| `multichain` | `edge_a → svc_a → svc_b → svc_c` and `edge_x → svc_x → svc_y → svc_c` | `edge_a_container`, `svc_a_container`, `svc_b_container`, `svc_c_container`, `edge_x_container`, `svc_x_container`, `svc_y_container` |

The application does nothing but spend time and call its one downstream. Every
node draws a lognormal service time from a **mounted** `node.yaml`
(`/opt/app/conf/predicted/node.yaml`, overridden by `NODE_CONFIG_PATH`) and
sleeps it; `RelayNode` hands its admission permit back with
`rpcpolicy.ReleasePermit(ctx)` before calling downstream, which is msim's
`hold_worker_during_fanout=False`. `EdgeNode` has no service time of its own:
the edge exists to own a client-side retry policy.

Every internal edge is `/grpc.NodeService/Call`, so a per-edge policy is the
calling container's own policy bundle.

The specs carry **no** `opentelemetry.Instrument`, `workload.Generator`,
`gotests.Test` or `clientpool`: the attempt log is the interceptor's own JSONL
file, the load comes from the harness's driver VMs, and a client pool would put
an unmodelled queue between the retry loop and the wire.

## Compile

```sh
export PATH="$HOME/go/bin:$PATH"          # protoc-gen-go, protoc-gen-go-grpc
cd examples/predicted/wiring
go run . -w single     -o build/single
go run . -w multichain -o build/multichain
```

## Gate K0a

The gate proves that the three codegen hooks of WP2 reached every generated
container, that every generated tree still compiles, and that the upstream
`examples/leaf` path is unaffected when the interceptor is inert.

```sh
export PATH="$HOME/go/bin:$PATH"
cd examples/predicted/wiring

# 1. compile both specs
rm -rf build
go run . -w single     -o build/single
go run . -w multichain -o build/multichain

# 2. the four greps, on each build tree.
#    The last grep is scoped to the GENERATED sources: build/<w>/**/runtime and
#    build/<w>/**/workflow are verbatim copies of this repo's own modules, whose
#    tests and unrelated plugins legitimately say `ctx := context.Background()`.
for w in single multichain; do
  grep -rq 'rpcpolicy.ServerOptions()' build/$w                 || { echo "FAIL $w: no ServerOptions"; exit 1; }
  grep -rq 'rpcpolicy.DialOptions()'   build/$w                 || { echo "FAIL $w: no DialOptions";   exit 1; }
  grep -rq 'time.ParseDuration("1s")'  build/$w                 && { echo "FAIL $w: ParseDuration(1s) survives"; exit 1; }
  test "$(grep -rn 'ctx := context.Background()' build/$w --include='*.go' \
            | grep -v '/runtime/' | grep -v '/workflow/' | wc -l)" -eq 0 \
                                                                || { echo "FAIL $w: generated code still starts a fresh context"; exit 1; }
  echo "greps OK: $w"
done

# 3. go build inside every generated go.work tree (one per container)
for w in single multichain; do
  for wk in $(find build/$w -name go.work); do
    d=$(dirname "$wk")
    mods=$(sed -n 's|^\s*\./\(.*\)$|./\1/...|p' "$wk" | tr '\n' ' ')
    (cd "$d" && go build $mods) || { echo "FAIL build $d"; exit 1; }
    echo "build OK: $d"
  done
done

# 4. a Dockerfile per expected container (CONTRACTS.md §7 names)
for c in edge_container svc_a_container; do
  test -f build/single/docker/$c/Dockerfile || { echo "FAIL single: no $c/Dockerfile"; exit 1; }
done
for c in edge_a_container svc_a_container svc_b_container svc_c_container \
         edge_x_container svc_x_container svc_y_container; do
  test -f build/multichain/docker/$c/Dockerfile || { echo "FAIL multichain: no $c/Dockerfile"; exit 1; }
done
echo "Dockerfiles OK"

# 5. the inert path: examples/leaf still compiles and builds unchanged
cd ../../leaf/wiring
rm -rf /tmp/leafbuild
go run . -w docker -o /tmp/leafbuild
for wk in $(find /tmp/leafbuild -name go.work); do
  d=$(dirname "$wk")
  mods=$(sed -n 's|^\s*\./\(.*\)$|./\1/...|p' "$wk" | tr '\n' ' ')
  (cd "$d" && go build $mods) || { echo "FAIL leaf build $d"; exit 1; }
done
echo "leaf OK"
```

Unit tests of the workflow module:

```sh
cd examples/predicted/workflow && go test ./...
```

## Local smoke of the Single stack (by hand, no harness)

Builds the two Single images and runs them under compose with a policy bundle,
a node config and (in the second half) an armed fault schedule. This is the
by-hand version of what `harness deploy build|render|conf|arm` will do.

```sh
cd examples/predicted/wiring/build/single/docker
docker build -t predicted-single-svc-a:smoke ./svc_a_container
docker build -t predicted-single-edge:smoke  ./edge_container

S=/tmp/predicted-smoke
mkdir -p $S/conf/svc_a_container/{rpcpolicy,predicted,faults} \
         $S/conf/edge_container/rpcpolicy \
         $S/logs/{svc_a_container,edge_container}
```

`$S/conf/svc_a_container/rpcpolicy/policy.yaml` — the admission station only:

```yaml
default_policy: none
profiles:
  none: {timeout: 1s, global_timeout: 0s}
server:
  workers: 16
  queue_capacity: 20
  strip_inbound_deadline: false
  hold_permit_through_fanout: false
```

`$S/conf/edge_container/rpcpolicy/policy.yaml` — the `top` profile of msim's
`examples/default.py`:

```yaml
default_policy: none
profiles:
  none: {timeout: 1s, global_timeout: 0s}
  top:
    timeout: 50ms
    global_timeout: 0s
    retry:
      enabled: true
      kind: fixed
      max_attempts: 2
      delay: 100ms
      retry_on: [Unavailable, DeadlineExceeded, ResourceExhausted, Aborted]
method_policies:
  "/grpc.NodeService/Call": top
```

`$S/conf/svc_a_container/predicted/node.yaml` = `benchmarks/single/node/node.yaml`
(20 ms median, sigma 0.5, `mode: sleep`).
`$S/conf/svc_a_container/faults/faults.yaml`:

```yaml
rules:
  - method: "/grpc.NodeService/Call"
    start_s: 0
    end_s: 3600
    add_latency_ms: 0
    p_fail: 1.0
```

`$S/docker-compose.yml` (the generated `docker/docker-compose.yml` with
`build:` replaced by `image:`, the conf/log bind mounts added, the interceptor's
environment set, and the edge's HTTP port published):

```yaml
services:
  edge_container:
    image: predicted-single-edge:smoke
    hostname: edge_container
    ports:  ["127.0.0.1:18080:2000"]
    environment:
     - EDGE_HTTP_BIND_ADDR=0.0.0.0:2000
     - SVC_A_GRPC_DIAL_ADDR=svc_a_container:12345
     - RPCPOLICY_CONFIG=/opt/app/conf/rpcpolicy/policy.yaml
     - RPCPOLICY_LOG_DIR=/var/log/rpcpolicy
     - OTEL_SERVICE_NAME=edge
     - FAULT_CONFIG_PATH=
     - FAULT_EPOCH_MS=
    volumes:
     - ./conf/edge_container/rpcpolicy:/opt/app/conf/rpcpolicy:ro
     - ./logs/edge_container:/var/log/rpcpolicy
    restart: "no"
    stop_grace_period: 2s
  svc_a_container:
    image: predicted-single-svc-a:smoke
    hostname: svc_a_container
    expose: ["12345"]
    environment:
     - SVC_A_GRPC_BIND_ADDR=0.0.0.0:12345
     - RPCPOLICY_CONFIG=/opt/app/conf/rpcpolicy/policy.yaml
     - NODE_CONFIG_PATH=/opt/app/conf/predicted/node.yaml
     - OTEL_SERVICE_NAME=svc-A
     - RPCPOLICY_LOG_DIR=/var/log/rpcpolicy
     - FAULT_CONFIG_PATH=
     - FAULT_EPOCH_MS=
    volumes:
     - ./conf/svc_a_container/rpcpolicy:/opt/app/conf/rpcpolicy:ro
     - ./conf/svc_a_container/predicted:/opt/app/conf/predicted:ro
     - ./conf/svc_a_container/faults:/opt/app/conf/faults:ro
     - ./logs/svc_a_container:/var/log/rpcpolicy
    restart: "no"
    stop_grace_period: 2s
```

Clean half:

```sh
cd $S && docker compose up -d
for i in 1 2 3; do
  TR=$(head -c16 /dev/urandom | xxd -p -c 32)
  SP=$(head -c8  /dev/urandom | xxd -p -c 16)
  curl -s -H "traceparent: 00-$TR-$SP-01" "http://localhost:18080/Root?key=$i"
done
sleep 2 && cat logs/edge_container/*.jsonl logs/svc_a_container/*.jsonl
```

Faulted half — `FAULT_CONFIG_PATH` and `FAULT_EPOCH_MS` are set on `svc_a`
**only** and both together (exactly one set panics at startup, by design), and
only that container is recreated:

```sh
cat > $S/docker-compose.override.yml <<EOF
services:
  svc_a_container:
    environment:
     - FAULT_CONFIG_PATH=/opt/app/conf/faults/faults.yaml
     - FAULT_EPOCH_MS=$(date +%s%3N)
EOF
docker compose up -d svc_a_container
TR=$(head -c16 /dev/urandom | xxd -p -c 32); SP=$(head -c8 /dev/urandom | xxd -p -c 16)
curl -s -H "traceparent: 00-$TR-$SP-01" "http://localhost:18080/Root?key=7"
sleep 2 && grep "$TR" logs/edge_container/*.jsonl logs/svc_a_container/*.jsonl
docker compose down
```

Expected (and observed on 2026-08-29): a clean probe returns `{"Ret0":1}` in
about 20 ms and writes a `root` → `client` → `server` chain whose
`parent_span_id`s link up, with `admission_outcome: admitted` and
`duration_ms ≈ 20`; a faulted probe returns HTTP 500 and writes **two** client
records, `attempt: 1` with `retry_delay_ms: 0` and `attempt: 2` with
`retry_delay_ms: 100` and `retry_denied: "exhausted"`, both
`response_code: "Unavailable"`, against two `server` records with
`fault_hit: true`.
