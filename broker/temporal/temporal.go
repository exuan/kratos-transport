package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	semConv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/tx7do/kratos-transport/broker"
	"github.com/tx7do/kratos-transport/tracing"
)

const (
	defaultAddr = "localhost:7233"
)

const (
	TracerMessageSystemKey = "temporal"
	SpanNameProducer       = "temporal-producer"
	SpanNameConsumer       = "temporal-consumer"
)

const (
	defaultNamespace       = "default"
	defaultWorkflowName    = "BrokerMessageWorkflow"
	processMessageActivity = "ProcessMessage"
	defaultActivityTimeout = time.Minute * 5
)

////////////////////////////////////////////////////////////////////////////////
/// Default Workflow & Activity
////////////////////////////////////////////////////////////////////////////////

// BrokerMessageWorkflow is a simple Temporal workflow that receives a message body
// and delegates processing to the registered activity.
// For complex multi-step orchestration, register a custom workflow via WithWorkflows.
func BrokerMessageWorkflow(ctx workflow.Context, body []byte) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: defaultActivityTimeout,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	return workflow.ExecuteActivity(ctx, processMessageActivity, body).Get(ctx, nil)
}

// processActivity wraps a broker handler and binder into a Temporal activity.
type processActivity struct {
	handler broker.Handler
	binder  broker.Binder
	b       *temporalBroker
	topic   string
}

// ProcessMessage is the activity implementation that deserializes the message
// and invokes the broker handler.
func (a *processActivity) ProcessMessage(ctx context.Context, body []byte) error {
	ctx, span := a.b.startConsumerSpan(ctx, a.topic)
	defer func() {
		a.b.finishConsumerSpan(ctx, span, nil)
	}()

	m := &broker.Message{
		Headers: make(broker.Headers),
	}

	if a.binder != nil {
		m.Body = a.binder()
		if err := broker.Unmarshal(a.b.options.Codec, body, &m.Body); err != nil {
			a.b.finishConsumerSpan(ctx, span, err)
			return err
		}
	} else {
		m.Body = body
	}

	p := &publication{
		m:     m,
		topic: a.topic,
	}

	if err := a.handler(ctx, p); err != nil {
		a.b.finishConsumerSpan(ctx, span, err)
		return err
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////
/// Broker
///////////////////////////////////////////////////////////////////////////////

type temporalBroker struct {
	options broker.Options
	client  client.Client

	subscribers *broker.SubscriberSyncMap

	producerTracer *tracing.Tracer
	consumerTracer *tracing.Tracer
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.NewOptionsAndApply(opts...)

	return &temporalBroker{
		options:     options,
		subscribers: broker.NewSubscriberSyncMap(),
	}
}

func (b *temporalBroker) Name() string {
	return "temporal"
}

func (b *temporalBroker) Options() broker.Options {
	if b.options.Context == nil {
		b.options.Context = context.Background()
	}
	return b.options
}

func (b *temporalBroker) Address() string {
	if len(b.options.Addrs) > 0 {
		return b.options.Addrs[0]
	}
	return ""
}

func (b *temporalBroker) Init(opts ...broker.Option) error {
	b.options.Apply(opts...)

	if len(b.options.Addrs) == 0 {
		b.options.Addrs = []string{defaultAddr}
	}

	if len(b.options.Tracings) > 0 {
		b.producerTracer = tracing.NewTracer(trace.SpanKindProducer, SpanNameProducer, b.options.Tracings...)
		b.consumerTracer = tracing.NewTracer(trace.SpanKindConsumer, SpanNameConsumer, b.options.Tracings...)
	}

	return nil
}

func (b *temporalBroker) Connect() error {
	namespace := defaultNamespace
	if v, ok := b.options.Context.Value(namespaceKey{}).(string); ok && v != "" {
		namespace = v
	}

	c, err := client.NewClient(client.Options{
		HostPort:  b.Address(),
		Namespace: namespace,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to temporal server at %s: %w", b.Address(), err)
	}

	b.client = c
	LogInfof("connected to temporal server at %s (namespace: %s)", b.Address(), namespace)

	return nil
}

func (b *temporalBroker) Disconnect() error {
	if b.client != nil {
		b.client.Close()
	}
	b.subscribers.Clear()
	return nil
}

///////////////////////////////////////////////////////////////////////////////
/// Request — Synchronous Workflow Execution (blocks until result)
///////////////////////////////////////////////////////////////////////////////

func (b *temporalBroker) Request(ctx context.Context, topic string, msg *broker.Message, opts ...broker.RequestOption) (*broker.Message, error) {
	if b.client == nil {
		return nil, errors.New("not connected")
	}

	reqOpts := broker.NewRequestOptions(opts...)

	buf, err := broker.Marshal(b.options.Codec, msg.Body)
	if err != nil {
		return nil, err
	}

	taskQueue := topic

	workflowOpts := client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}

	applyRequestWorkflowOptions(&workflowOpts, reqOpts.Context)

	workflowFn := any(BrokerMessageWorkflow)
	if v, ok := reqOpts.Context.Value(workflowFnKey{}).(any); ok && v != nil {
		workflowFn = v
	}

	var span trace.Span
	ctx, span = b.startProducerSpan(reqOpts.Context, taskQueue)

	we, err := b.client.ExecuteWorkflow(ctx, workflowOpts, workflowFn, buf)
	if err != nil {
		b.finishProducerSpan(ctx, span, err)
		return nil, err
	}

	// Block until the workflow completes and decode the result.
	var result []byte
	if err = we.Get(ctx, &result); err != nil {
		b.finishProducerSpan(ctx, span, err)
		return nil, err
	}

	b.finishProducerSpan(ctx, span, nil)

	return &broker.Message{
		Body: result,
		Headers: broker.Headers{
			"workflow-id": we.GetID(),
			"run-id":      we.GetRunID(),
		},
	}, nil
}

///////////////////////////////////////////////////////////////////////////////
/// Publish — Asynchronous Workflow Execution (fire-and-forget)
///////////////////////////////////////////////////////////////////////////////

func (b *temporalBroker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	var finalTask = b.internalPublish

	if len(b.options.PublishMiddlewares) > 0 {
		finalTask = broker.ChainPublishMiddleware(finalTask, b.options.PublishMiddlewares)
	}

	return finalTask(ctx, topic, msg, opts...)
}

func (b *temporalBroker) internalPublish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	buf, err := broker.Marshal(b.options.Codec, msg.Body)
	if err != nil {
		return err
	}

	return b.publish(ctx, topic, buf, opts...)
}

func (b *temporalBroker) publish(ctx context.Context, topic string, body []byte, opts ...broker.PublishOption) error {
	if b.client == nil {
		return errors.New("not connected")
	}

	publishOpts := broker.NewPublishOptions(opts...)

	taskQueue := topic
	if v, ok := publishOpts.Context.Value(taskQueueKey{}).(string); ok && v != "" {
		taskQueue = v
	}

	workflowOpts := client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}

	applyPublishWorkflowOptions(&workflowOpts, publishOpts.Context)

	// Select workflow function
	workflowFn := any(BrokerMessageWorkflow)
	if v, ok := publishOpts.Context.Value(workflowFnKey{}).(any); ok && v != nil {
		workflowFn = v
	}

	var span trace.Span
	ctx, span = b.startProducerSpan(publishOpts.Context, taskQueue)

	_, err := b.client.ExecuteWorkflow(ctx, workflowOpts, workflowFn, body)

	b.finishProducerSpan(ctx, span, err)

	return err
}

///////////////////////////////////////////////////////////////////////////////
/// Subscribe — Worker with Workflow/Activity Registration
///////////////////////////////////////////////////////////////////////////////

func (b *temporalBroker) Subscribe(topic string, handler broker.Handler, binder broker.Binder, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	if b.client == nil {
		return nil, errors.New("not connected")
	}

	options := broker.NewSubscribeOptions(opts...)

	if len(b.options.SubscriberMiddlewares) > 0 {
		handler = broker.ChainSubscriberMiddleware(handler, b.options.SubscriberMiddlewares)
	}

	taskQueue := topic

	// Build worker options
	wOpts := worker.Options{}
	if v, ok := options.Context.Value(workerOptsKey{}).(worker.Options); ok {
		wOpts = v
	}

	w := worker.New(b.client, taskQueue, wOpts)

	// Always register the default BrokerMessageWorkflow
	w.RegisterWorkflow(BrokerMessageWorkflow)

	// Register additional user-defined workflows
	if v, ok := options.Context.Value(workflowsKey{}).([]any); ok {
		for _, wf := range v {
			w.RegisterWorkflow(wf)
		}
	}

	// Register the broker handler activity
	act := &processActivity{
		handler: handler,
		binder:  binder,
		b:       b,
		topic:   topic,
	}
	w.RegisterActivity(act.ProcessMessage)

	// Register additional user-defined activities
	if v, ok := options.Context.Value(activitiesKey{}).([]any); ok {
		for _, a := range v {
			w.RegisterActivity(a)
		}
	}

	// Start the worker
	if err := w.Start(); err != nil {
		return nil, fmt.Errorf("failed to start temporal worker for task queue %s: %w", taskQueue, err)
	}

	LogInfof("started temporal worker for task queue: %s", taskQueue)

	subs := &subscriber{
		b:       b,
		worker:  w,
		topic:   topic,
		options: options,
	}

	b.subscribers.Add(topic, subs)

	return subs, nil
}

////////////////////////////////////////////////////////////////////////////////
/// Temporal Client Accessor
////////////////////////////////////////////////////////////////////////////////

// Client returns the underlying Temporal client for advanced usage
// (e.g., SignalWorkflow, QueryWorkflow, DescribeWorkflowExecution).
func (b *temporalBroker) Client() client.Client {
	return b.client
}

////////////////////////////////////////////////////////////////////////////////
/// Shared Workflow Options Builders
////////////////////////////////////////////////////////////////////////////////

func applyPublishWorkflowOptions(opts *client.StartWorkflowOptions, ctx context.Context) {
	if v, ok := ctx.Value(workflowIDKey{}).(string); ok && v != "" {
		opts.ID = v
	}
	if v, ok := ctx.Value(workflowStartTimeoutKey{}).(time.Duration); ok && v > 0 {
		opts.WorkflowTaskTimeout = v
	}
	if v, ok := ctx.Value(workflowRunTimeoutKey{}).(time.Duration); ok && v > 0 {
		opts.WorkflowRunTimeout = v
	}
	if v, ok := ctx.Value(workflowExecutionTimeoutKey{}).(time.Duration); ok && v > 0 {
		opts.WorkflowExecutionTimeout = v
	}
	if v, ok := ctx.Value(workflowRetryPolicyKey{}).(*temporal.RetryPolicy); ok {
		opts.RetryPolicy = v
	}
	if v, ok := ctx.Value(workflowCronKey{}).(string); ok && v != "" {
		opts.CronSchedule = v
	}
	if v, ok := ctx.Value(workflowIDReusePolicyKey{}).(enumspb.WorkflowIdReusePolicy); ok {
		opts.WorkflowIDReusePolicy = v
	}
}

func applyRequestWorkflowOptions(opts *client.StartWorkflowOptions, ctx context.Context) {
	if v, ok := ctx.Value(workflowIDKey{}).(string); ok && v != "" {
		opts.ID = v
	}
	if v, ok := ctx.Value(workflowStartTimeoutKey{}).(time.Duration); ok && v > 0 {
		opts.WorkflowTaskTimeout = v
	}
	if v, ok := ctx.Value(workflowRunTimeoutKey{}).(time.Duration); ok && v > 0 {
		opts.WorkflowRunTimeout = v
	}
	if v, ok := ctx.Value(workflowExecutionTimeoutKey{}).(time.Duration); ok && v > 0 {
		opts.WorkflowExecutionTimeout = v
	}
	if v, ok := ctx.Value(workflowRetryPolicyKey{}).(*temporal.RetryPolicy); ok {
		opts.RetryPolicy = v
	}
	if v, ok := ctx.Value(workflowIDReusePolicyKey{}).(enumspb.WorkflowIdReusePolicy); ok {
		opts.WorkflowIDReusePolicy = v
	}
}

///////////////////////////////////////////////////////////////////////////////
/// Tracing
///////////////////////////////////////////////////////////////////////////////

func (b *temporalBroker) startProducerSpan(ctx context.Context, topic string) (context.Context, trace.Span) {
	if b.producerTracer == nil {
		return ctx, nil
	}

	carrier := newMapCarrier()

	attrs := []attribute.KeyValue{
		semConv.MessagingSystemKey.String(TracerMessageSystemKey),
		semConv.MessagingDestinationKindTopic,
		semConv.MessagingDestinationKey.String(topic),
	}

	ctx, span := b.producerTracer.Start(ctx, carrier, attrs...)

	return ctx, span
}

func (b *temporalBroker) finishProducerSpan(ctx context.Context, span trace.Span, err error) {
	if b.producerTracer == nil {
		return
	}

	b.producerTracer.End(ctx, span, err)
}

func (b *temporalBroker) startConsumerSpan(ctx context.Context, topic string) (context.Context, trace.Span) {
	if b.consumerTracer == nil {
		return ctx, nil
	}

	carrier := newMapCarrier()

	attrs := []attribute.KeyValue{
		semConv.MessagingSystemKey.String(TracerMessageSystemKey),
		semConv.MessagingDestinationKindTopic,
		semConv.MessagingDestinationKey.String(topic),
		semConv.MessagingOperationReceive,
	}

	ctx, span := b.consumerTracer.Start(ctx, carrier, attrs...)

	return ctx, span
}

func (b *temporalBroker) finishConsumerSpan(ctx context.Context, span trace.Span, err error) {
	if b.consumerTracer == nil {
		return
	}

	b.consumerTracer.End(ctx, span, err)
}
