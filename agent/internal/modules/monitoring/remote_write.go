package monitoring

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func parsePrometheusText(reader io.Reader, timestamp time.Time, externalLabels map[string]string) ([]prompb.TimeSeries, error) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(reader)
	if err != nil {
		return nil, fmt.Errorf("parse node_exporter metrics: %w", err)
	}

	timestampMs := timestamp.UnixMilli()
	series := make([]prompb.TimeSeries, 0)
	for name, family := range families {
		for _, metric := range family.Metric {
			value, ok := metricValue(family.GetType(), metric)
			if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
				continue
			}
			labels := labelsForMetric(name, metric, externalLabels)
			series = append(series, prompb.TimeSeries{
				Labels: labels,
				Samples: []prompb.Sample{
					{
						Value:     value,
						Timestamp: timestampMs,
					},
				},
			})
		}
	}
	return series, nil
}

func metricValue(metricType dto.MetricType, metric *dto.Metric) (float64, bool) {
	switch metricType {
	case dto.MetricType_COUNTER:
		if metric.Counter == nil || metric.Counter.Value == nil {
			return 0, false
		}
		return metric.Counter.GetValue(), true
	case dto.MetricType_GAUGE:
		if metric.Gauge == nil || metric.Gauge.Value == nil {
			return 0, false
		}
		return metric.Gauge.GetValue(), true
	case dto.MetricType_UNTYPED:
		if metric.Untyped == nil || metric.Untyped.Value == nil {
			return 0, false
		}
		return metric.Untyped.GetValue(), true
	default:
		return 0, false
	}
}

func labelsForMetric(metricName string, metric *dto.Metric, externalLabels map[string]string) []prompb.Label {
	labelsByName := map[string]string{"__name__": metricName}
	for _, label := range metric.Label {
		name := label.GetName()
		if name != "" {
			labelsByName[name] = label.GetValue()
		}
	}
	for name, value := range externalLabels {
		if name != "" && value != "" {
			labelsByName[name] = value
		}
	}

	names := make([]string, 0, len(labelsByName))
	for name := range labelsByName {
		names = append(names, name)
	}
	sort.Strings(names)

	labels := make([]prompb.Label, 0, len(names))
	for _, name := range names {
		labels = append(labels, prompb.Label{Name: name, Value: labelsByName[name]})
	}
	return labels
}

func remoteWrite(ctx context.Context, client *http.Client, url string, apiKey string, series []prompb.TimeSeries) error {
	if apiKey == "" {
		return fmt.Errorf("agent API key is required for metrics ingest")
	}
	body, err := proto.Marshal(&prompb.WriteRequest{Timeseries: series})
	if err != nil {
		return fmt.Errorf("encode remote-write payload: %w", err)
	}
	compressed := snappy.Encode(nil, body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(compressed))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("remote-write node metrics: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("remote-write node metrics: %s: %s", resp.Status, string(raw))
	}
	return nil
}
