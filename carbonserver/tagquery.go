package carbonserver

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap/zapcore"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	tindex "github.com/lomik/go-carbon/tags/index"
	//	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	//	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

type tagValue struct {
	Count int32
	Value string
}

type statTagResponse struct {
	Tag    string
	Values []tagValue
}

type listTagsResponse struct {
	Tags []tagType
}

type tagType struct {
	Tag string
}

func (listener *CarbonserverListener) statTagHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /tags/datacenter?format=pickle&pretty=1&filter=data

	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.StatTag, 1)

	req.ParseForm()
	format := req.FormValue("format")
	filter := req.FormValue("filter")

	var tag string
	if strings.Count(req.URL.Path, "/") >= 2 {
		tag = req.URL.Path[6:]
	}

	var limit = 100
	if num, err := strconv.Atoi(req.FormValue("limit")); err == nil {
		limit = num
	}

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "statTag"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("format", format),
		zap.String("filter", filter),
	))

	if format != "json" && format != "pickle" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.StatTagErrors, 1)
		accessLogger.Error("statTag failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	if tag == "" {
		atomic.AddUint64(&listener.metrics.StatTagErrors, 1)
		accessLogger.Error("statTag failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "empty tag name"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (no query)", http.StatusBadRequest)
		return
	}

	var err error
	var contentType string
	var data = []byte("{}")
	stat := listener.tagsIdx.StatTag(tag, filter, limit)
	if stat != nil {
		var resp statTagResponse
		resp.Tag = stat.Tag
		for _, val := range stat.Values {
			resp.Values = append(resp.Values, tagValue{Count: int32(val.Count), Value: val.Value})
		}
		switch format {
		case "json":
			contentType = "application/json"
			data, err = json.Marshal(resp)
			/*
			   case "protobuf", "protobuf3":
			   contentType = "application/protobuf"
			   data, err = resp.Marshal()
			*/
		}
	}

	if err != nil {
		accessLogger.Error("statTag failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "internal error while processing request"),
			zap.Error(err),
			zap.Int("http_code", http.StatusInternalServerError),
		)
		http.Error(wr, fmt.Sprintf("Internal error while processing request (%v)", err),
			http.StatusInternalServerError)
		return
	}

	wr.Header().Set("Content-Type", contentType)
	wr.Write(data)

	accessLogger.Info("statTag success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.String("tag", tag),
		zap.String("filter", filter),
		zap.Int("http_code", http.StatusOK),
	)
	return
}

func (listener *CarbonserverListener) listTagsHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /tags/?format=pickle&pretty=1&filter=data

	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.FindTags, 1)

	req.ParseForm()
	format := req.FormValue("format")
	filter := req.FormValue("filter")

	var limit = 100
	if num, err := strconv.Atoi(req.FormValue("limit")); err == nil {
		limit = num
	}

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "listTags"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("format", format),
		zap.String("filter", filter),
	))

	if format != "json" && format != "pickle" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.FindTagsErrors, 1)
		accessLogger.Error("listTags failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	var err error
	var data = []byte(`{}`)
	var contentType string
	tags := listener.tagsIdx.ListTags(filter, limit)
	var resp listTagsResponse
	for _, tag := range tags {
		resp.Tags = append(resp.Tags, tagType{Tag: tag})
	}
	switch format {
	case "json":
		contentType = "application/json"
		data, err = json.Marshal(resp)
		/*
		   case "protobuf", "protobuf3":
		   contentType = "application/protobuf"
		   data, err = resp.Marshal()
		*/
	}

	if err != nil {
		accessLogger.Error("listTags failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "internal error while processing request"),
			zap.Error(err),
			zap.Int("http_code", http.StatusInternalServerError),
		)
		http.Error(wr, fmt.Sprintf("Internal error while processing request (%v)", err),
			http.StatusInternalServerError)
		return
	}

	wr.Header().Set("Content-Type", contentType)
	wr.Write(data)

	accessLogger.Info("listTags success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("tags", len(tags)),
		zap.String("filter", filter),
		zap.Int("http_code", http.StatusOK),
	)
	return
}

func (listener *CarbonserverListener) seriesByTagHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /seriesByTag?format=pickle&pretty=1&filter=data

	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.SeriesByTag, 1)

	req.ParseForm()

	from := req.FormValue("from")
	until := req.FormValue("until")

	var metricExpr *tindex.TagValueExpr
	if req.FormValue("metricExpr") != "" {
		metricExpr = tindex.NewTagValueExpr(req.FormValue("metricExpr"))
	}

	var tagValues []*tindex.TagValueExpr
	for _, expr := range req.Form["tagValues"] {
		tagValues = append(tagValues, tindex.NewTagValueExpr(expr))
	}

	var limit = 100
	if num, err := strconv.Atoi(req.FormValue("limit")); err == nil {
		limit = num
	}

	zapFields := []zapcore.Field{
		zap.String("handler", "seriesByTag"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		// zap.String("format", format),
		zap.String("from", from),
		zap.String("until", until),
		zap.String("metricExpr", req.FormValue("metricExpr")),
		zap.Strings("tagValues", req.Form["tagValues"]),
	}
	logger := TraceContextToZap(ctx, listener.logger.With(zapFields...))
	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(zapFields...))

	format, err := getFormat(req)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, fmt.Sprintf("Bad request: %s", err), http.StatusBadRequest)
		return
	}

	targets, err := getTargets(req, format)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, fmt.Sprintf("Bad request: %s", err), http.StatusBadRequest)
		return
	}

	tgs := getTargetNames(targets)
	accessLogger = accessLogger.With(
		zap.Strings("targets", tgs),
	)

	if len(tagValues) == 0 {
		atomic.AddUint64(&listener.metrics.SeriesByTagErrors, 1)
		accessLogger.Error("seriesByTag failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "empty tagValues"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (no query)", http.StatusBadRequest)
		return
	}

	metrics := listener.tagsIdx.ListMetrics(metricExpr, tagValues, limit)
	paths := make([]string, 0, len(metrics))
	for _, m := range metrics {
		paths = append(paths, m.Path)
	}

	response, _, err := listener.fetchWithCache(logger, format, targets)

	if err != nil {
		accessLogger.Error("seriesByTag failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "internal error while processing request"),
			zap.Error(err),
			zap.Int("http_code", http.StatusInternalServerError),
		)
		http.Error(wr, fmt.Sprintf("Internal error while processing request (%v)", err),
			http.StatusInternalServerError)
		return
	}

	wr.Header().Set("Content-Type", response.contentType)
	wr.Write(response.data)

	accessLogger.Info("seriesByTag success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("metrics_size", len(metrics)),
		zap.Int("http_code", http.StatusOK),
	)
	return
}
