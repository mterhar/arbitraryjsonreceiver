// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package arbitraryjsonreceiver // import "go.opentelemetry.io/collector/receiver/otlpreceiver"

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/arbitraryjsonreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/arbitraryjsonreceiver/internal/sharedcomponent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	httpPort = 8080

	defaultTracesURLPath = "/"
)

// NewFactory creates a new OTLP receiver factory.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithTraces(createTraces, metadata.TracesStability),
	)
}

// createDefaultConfig creates the default configuration for receiver.
func createDefaultConfig() component.Config {
	starttimeFieldsArr := []string{"time"}
	durationFieldsArr := []string{"duration_ms"}
	attributesFromArr := []string{"data"}
	return &Config{
		HTTP: &HTTPConfig{
			ServerConfig: &confighttp.ServerConfig{
				Endpoint: "localhost:" + string(httpPort),
			},
			TracesURLPath: defaultTracesURLPath,
		},
		Wrapper: "array",
		Resources: ResourcesConfig{
			ServiceName: "service.name",
		},
		Attributes: AttributesConfig{
			SampleRate:      "samplerate",
			TraceId:         "trace.trace_id",
			SpanId:          "trace.span_id",
			ParentId:        "trace.parent_id",
			Name:            "name",
			SpanKind:        "span.kind",
			StarttimeFields: starttimeFieldsArr,
			DurationFields:  durationFieldsArr,
		},
		AttributesFrom: attributesFromArr,
	}
}

// createTraces creates a trace receiver based on provided config.
func createTraces(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	oCfg := cfg.(*Config)
	r, err := receivers.LoadOrStore(
		oCfg,
		func() (*arbitraryjsonReceiver, error) {
			return newarbitraryjsonReceiver(oCfg, &set)
		},
		&set.TelemetrySettings,
	)
	if err != nil {
		return nil, err
	}

	r.Unwrap().registerTraceConsumer(nextConsumer)
	return r, nil
}

// This is the map of already created OTLP receivers for particular configurations.
// We maintain this map because the Factory is asked trace and metric receivers separately
// when it gets CreateTracesReceiver() and CreateMetricsReceiver() but they must not
// create separate objects, they must use one otlpReceiver object per configuration.
// When the receiver is shutdown it should be removed from this map so the same configuration
// can be recreated successfully.
var receivers = sharedcomponent.NewMap[*Config, *arbitraryjsonReceiver]()
