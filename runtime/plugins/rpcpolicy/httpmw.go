package rpcpolicy

import (
	"net/http"
)

// The front-door HTTP middleware: it reads the driver's traceparent, stamps the
// route key every downstream gRPC call carries, and writes the ROOT record that
// the pipeline joins to the driver's client.csv by trace id.

// HTTPMiddleware returns the mux middleware generated HTTP servers install.
// Identity when inert.
func HTTPMiddleware() func(http.Handler) http.Handler {
	st := currentState()
	if st.inert {
		return func(next http.Handler) http.Handler { return next }
	}
	return st.httpMiddleware()
}

func (s *runtimeState) httpMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reg := s.registry.Load()

			traceID, parentSpanID := "", ""
			if tp := r.Header.Get(TraceparentKey); tp != "" {
				if t, sp, ok := parseTraceparent(tp); ok {
					traceID, parentSpanID = t, sp
				}
			}
			if traceID == "" {
				traceID = newTraceID()
			}
			spanID := newSpanID()
			route := reg.RouteFor(r.URL.Path)

			ctx := withTraceCtx(r.Context(), traceCtx{traceID: traceID, spanID: spanID, route: route})
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			start := s.clock.Now()
			next.ServeHTTP(sw, r.WithContext(ctx))
			end := s.clock.Now()

			// The root record is the only place the driver's HTTP status and
			// the trace id meet; root_metrics joins on trace_id.
			_ = s.log.Write(&RootRecord{
				baseRecord: baseRecord{
					Kind: "root", TraceID: traceID, SpanID: spanID, ParentSpanID: parentSpanID,
					Service: s.service, Operation: "HTTP " + r.Method + " " + r.URL.Path,
					StartEpoch: epochOf(start), EndEpoch: epochOf(end),
					DurationMS:   msOf(end.Sub(start)),
					IsError:      sw.status < 200 || sw.status >= 300,
					ResponseCode: httpCodeName(sw.status),
				},
				Route:      route,
				HTTPStatus: sw.status,
			})
		})
	}
}

// statusWriter captures the status code the handler wrote. A handler that never
// calls WriteHeader has written 200 by the time ServeHTTP returns.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(b)
}

// httpCodeName gives the root record a `response_code` in the same vocabulary
// as the gRPC records, so the pipeline's is_error / response_code columns are
// one type across all three record kinds.
func httpCodeName(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "OK"
	case status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout:
		return "DeadlineExceeded"
	case status == http.StatusTooManyRequests:
		return "ResourceExhausted"
	case status == http.StatusServiceUnavailable:
		return "Unavailable"
	default:
		return "Unknown"
	}
}
