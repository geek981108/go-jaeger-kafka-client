package test

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"go-jaeger-kafka-client/transport"
	"testing"
	"time"
)

func TestExampleWithValues(t *testing.T) {
	KC := transport.KafkaConnect{
		Addresses: []string{"localhost:9092"},
		Topic:     "jaeger-open-tracing",
		Spans:     nil,
		Process:   nil,
	}

	tracer, closer := jaeger.NewTracer(
		"testServiceName",
		jaeger.NewConstSampler(true),
		jaeger.NewRemoteReporter(&KC),
	)

	opentracing.SetGlobalTracer(tracer)
	var ctx = context.TODO()

	span1, ctx := opentracing.StartSpanFromContext(ctx, "span_1")
	time.Sleep(time.Second / 2)

	span11, _ := opentracing.StartSpanFromContext(ctx, "span_1-1")
	time.Sleep(time.Second / 2)
	span11.Finish()
	span1.Finish()
	defer closer.Close()
}
