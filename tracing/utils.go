package tracing

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/exporters/zipkin"

	"go.opentelemetry.io/otel/sdk/resource"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"

	semConv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

// NewExporter 创建一个导出器，支持：zipkin、otlp-http、otlp-grpc
func NewExporter(exporterName, endpoint string, insecure bool) (traceSdk.SpanExporter, error) {
	ctx := context.Background()

	switch exporterName {
	case "zipkin":
		return NewZipkinExporter(ctx, endpoint)
	case "jaeger":
		return nil, errors.New("jaeger exporter is no longer supported, please use otlp-http or otlp-grpc replace it")
	case "otlp-http":
		return NewOtlpHttpExporter(ctx, endpoint, insecure)
	case "otlp-grpc":
		return NewOtlpGrpcExporter(ctx, endpoint, insecure)
	default:
		fallthrough
	case "stdout":
		return stdouttrace.New()
	}
}

// NewZipkinExporter 创建一个zipkin导出器，默认对端地址：http://localhost:9411/api/v2/spans
func NewZipkinExporter(_ context.Context, endpoint string) (traceSdk.SpanExporter, error) {
	return zipkin.New(endpoint)
}

//// NewJaegerExporter 创建一个jaeger导出器，默认对端地址：http://localhost:14268/api/traces
//func NewJaegerExporter(_ context.Context, endpoint string) (traceSdk.SpanExporter, error) {
//	return jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
//}

// NewOtlpHttpExporter 创建OTLP/HTTP导出器，默认端口：4318
func NewOtlpHttpExporter(ctx context.Context, endpoint string, insecure bool, options ...otlptracehttp.Option) (traceSdk.SpanExporter, error) {
	var opts []otlptracehttp.Option
	opts = append(opts, otlptracehttp.WithEndpoint(endpoint))

	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	opts = append(opts, options...)

	return otlptrace.New(
		ctx,
		otlptracehttp.NewClient(opts...),
	)
}

// NewOtlpGrpcExporter 创建OTLP/gRPC导出器，默认端口：4317
func NewOtlpGrpcExporter(ctx context.Context, endpoint string, insecure bool, options ...otlptracegrpc.Option) (traceSdk.SpanExporter, error) {
	var opts []otlptracegrpc.Option
	opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))

	if insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	opts = append(opts, options...)

	return otlptrace.New(
		ctx,
		otlptracegrpc.NewClient(opts...),
	)
}

// NewTracerProvider 创建一个链路追踪器
func NewTracerProvider(exporterName, endpoint, serviceName, instanceId, version string, sampler float64) *traceSdk.TracerProvider {
	if instanceId == "" {
		ud, _ := uuid.NewUUID()
		instanceId = ud.String()
	}
	if version == "" {
		version = "x.x.x"
	}

	opts := []traceSdk.TracerProviderOption{
		traceSdk.WithSampler(traceSdk.ParentBased(traceSdk.TraceIDRatioBased(sampler))),
		traceSdk.WithResource(resource.NewSchemaless(
			semConv.ServiceNameKey.String(serviceName),
			semConv.ServiceInstanceIDKey.String(instanceId),
			semConv.ServiceVersionKey.String(version),
		)),
	}

	if len(endpoint) > 0 {
		exp, err := NewExporter(exporterName, endpoint, true)
		if err != nil {
			panic(err)
		}

		opts = append(opts, traceSdk.WithBatcher(exp))
	}

	return traceSdk.NewTracerProvider(opts...)
}
