package carbonserver

import (
	"encoding/json"
	"fmt"
	"github.com/lomik/go-carbon/tags"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	//	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	//	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

type tagNameValue struct {
	TagName string
	TagValue string
}

func (listener *CarbonserverListener) splitMetricTags(metric string) (string, []tagNameValue) {
	splittedString := strings.Split(metric, ";")

	if len(splittedString) == 1 {
		return splittedString[0], []tagNameValue{}
	}

	tagValues := make([]tagNameValue, 0, len(splittedString)-1)
	for _, tv := range splittedString[1:] {
		tvSplitted := strings.Split(tv, "=")
		tagValues = append(tagValues, tagNameValue{tvSplitted[0], tvSplitted[1]})
	}

	return splittedString[0], tagValues
}

func (listener *CarbonserverListener) tagMultiSeriesHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: tags/tagMultiSeries
	// --data-urlencode 'path=disk.used;rack=a1;datacenter=dc1;server=web01' \
	// --data-urlencode 'path=disk.used;rack=a1;datacenter=dc1;server=web02' \
	// --data-urlencode 'pretty=1'
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.SeriesByTag, 1)

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "statTag"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
	))

	err := req.ParseForm()
	if err != nil {
		accessLogger.Error("tagMultiSeries failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, fmt.Sprintf("Bad request: %s", err), http.StatusBadRequest)
		return
	}

	format, err := getFormat(req)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Error("tagMultiSeries failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, fmt.Sprintf("Bad request: %s", err), http.StatusBadRequest)
		return
	}

	accessLogger = accessLogger.With(
		zap.String("format", format.String()),
	)

	addedSeries := make([]string, 0)

	for _, path := range req.PostForm["path"] {
		fileName := tags.FilePath(listener.whisperData, path, listener.hashOnly)
		metric, tagValues := listener.splitMetricTags(path)
		if len(tagValues) == 0 {
			listener.logger.Warn("metric path contained no tags",
				zap.String("handler", "tagMultipleSeries"),
				zap.String("path", path),
			)
			continue
		}
		for _, tv := range tagValues {
			listener.tagsIdx.Insert(path, tv.TagName, tv.TagValue, metric, fileName)
		}
		addedSeries = append(addedSeries, path)
	}

	listener.logger.Info("path", zap.Any("path", req.PostForm))

	contentType :=  "application/json"
	data, err := json.Marshal(addedSeries)

	if err != nil {
		accessLogger.Error("tagMultiSeries failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusInternalServerError),
		)
		http.Error(wr, fmt.Sprintf("Internal error while processing request: %v", err),
			http.StatusInternalServerError)
		return
	}

	wr.Header().Set("Content-Type", contentType)
	_, err = wr.Write(data)
	if err != nil {
		accessLogger.Error("tagMultiSeries failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", err.Error()),
			zap.Int("http_code", http.StatusInternalServerError),
		)
		http.Error(wr, fmt.Sprintf("Internal error while processing request: %v", err),
			http.StatusInternalServerError)
		return
	}

	accessLogger.Info("tagMultiSeries success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
	return
}
