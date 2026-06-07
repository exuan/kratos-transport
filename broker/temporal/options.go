package temporal

import (
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/tx7do/kratos-transport/broker"
)

///////////////////////////////////////////////////////////////////////////////
/// Broker Options
///////////////////////////////////////////////////////////////////////////////

type namespaceKey struct{}

// WithNamespace sets the Temporal namespace (default: "default").
func WithNamespace(namespace string) broker.Option {
	return broker.OptionContextWithValue(namespaceKey{}, namespace)
}

///////////////////////////////////////////////////////////////////////////////
/// Shared Workflow Option Context Keys
// Used by both Publish (PublishOption) and Request (RequestOption).
///////////////////////////////////////////////////////////////////////////////

type taskQueueKey struct{}
type workflowIDKey struct{}
type workflowFnKey struct{}
type workflowStartTimeoutKey struct{}
type workflowRunTimeoutKey struct{}
type workflowExecutionTimeoutKey struct{}
type workflowRetryPolicyKey struct{}
type workflowCronKey struct{}
type workflowIDReusePolicyKey struct{}

///////////////////////////////////////////////////////////////////////////////
/// Publish Options (Workflow Execution)
///////////////////////////////////////////////////////////////////////////////

// WithTaskQueue overrides the task queue for publishing.
// By default, the topic is used as the task queue name.
func WithTaskQueue(taskQueue string) broker.PublishOption {
	return broker.PublishContextWithValue(taskQueueKey{}, taskQueue)
}

// WithWorkflowID sets a specific workflow ID.
func WithWorkflowID(id string) broker.PublishOption {
	return broker.PublishContextWithValue(workflowIDKey{}, id)
}

// WithWorkflowFn sets a custom workflow function to execute.
// The function must be a valid Temporal workflow function.
// By default, BrokerMessageWorkflow is used.
func WithWorkflowFn(fn any) broker.PublishOption {
	return broker.PublishContextWithValue(workflowFnKey{}, fn)
}

// WithStartTimeout sets the timeout for starting the workflow.
func WithStartTimeout(d time.Duration) broker.PublishOption {
	return broker.PublishContextWithValue(workflowStartTimeoutKey{}, d)
}

// WithRunTimeout sets the maximum time a single workflow run is allowed to execute.
func WithRunTimeout(d time.Duration) broker.PublishOption {
	return broker.PublishContextWithValue(workflowRunTimeoutKey{}, d)
}

// WithExecutionTimeout sets the maximum time that the workflow can be running
// (including retries and continue-as-new).
func WithExecutionTimeout(d time.Duration) broker.PublishOption {
	return broker.PublishContextWithValue(workflowExecutionTimeoutKey{}, d)
}

// WithRetryPolicy sets the retry policy for the workflow.
func WithRetryPolicy(policy *temporal.RetryPolicy) broker.PublishOption {
	return broker.PublishContextWithValue(workflowRetryPolicyKey{}, policy)
}

// WithCronSchedule sets a cron schedule for the workflow.
// When set, the workflow will be started according to the cron schedule.
func WithCronSchedule(cron string) broker.PublishOption {
	return broker.PublishContextWithValue(workflowCronKey{}, cron)
}

// WithIDReusePolicy sets the workflow ID reuse policy.
func WithIDReusePolicy(policy enumspb.WorkflowIdReusePolicy) broker.PublishOption {
	return broker.PublishContextWithValue(workflowIDReusePolicyKey{}, policy)
}

///////////////////////////////////////////////////////////////////////////////
/// Request Options (Synchronous Workflow Execution)
// These mirror the Publish options but use RequestOption type.
///////////////////////////////////////////////////////////////////////////////

// WithRequestWorkflowID sets a specific workflow ID for Request.
func WithRequestWorkflowID(id string) broker.RequestOption {
	return broker.RequestContextWithValue(workflowIDKey{}, id)
}

// WithRequestRunTimeout sets the run timeout for Request.
func WithRequestRunTimeout(d time.Duration) broker.RequestOption {
	return broker.RequestContextWithValue(workflowRunTimeoutKey{}, d)
}

// WithRequestExecutionTimeout sets the execution timeout for Request.
func WithRequestExecutionTimeout(d time.Duration) broker.RequestOption {
	return broker.RequestContextWithValue(workflowExecutionTimeoutKey{}, d)
}

// WithRequestStartTimeout sets the start timeout for Request.
func WithRequestStartTimeout(d time.Duration) broker.RequestOption {
	return broker.RequestContextWithValue(workflowStartTimeoutKey{}, d)
}

// WithRequestRetryPolicy sets the retry policy for Request.
func WithRequestRetryPolicy(policy *temporal.RetryPolicy) broker.RequestOption {
	return broker.RequestContextWithValue(workflowRetryPolicyKey{}, policy)
}

// WithRequestWorkflowFn sets a custom workflow function for Request.
func WithRequestWorkflowFn(fn any) broker.RequestOption {
	return broker.RequestContextWithValue(workflowFnKey{}, fn)
}

///////////////////////////////////////////////////////////////////////////////
/// Subscribe Options (Worker Configuration)
///////////////////////////////////////////////////////////////////////////////

type workerOptsKey struct{}
type workflowsKey struct{}
type activitiesKey struct{}

// WithWorkerOptions sets custom Temporal worker options.
func WithWorkerOptions(opts worker.Options) broker.SubscribeOption {
	return broker.SubscribeContextWithValue(workerOptsKey{}, opts)
}

// WithWorkflows registers additional workflow functions on the worker.
// BrokerMessageWorkflow is always registered by default.
func WithWorkflows(workflows ...any) broker.SubscribeOption {
	return broker.SubscribeContextWithValue(workflowsKey{}, workflows)
}

// WithActivities registers additional activity functions or structs on the worker.
// The broker handler activity is always registered by default.
func WithActivities(activities ...any) broker.SubscribeOption {
	return broker.SubscribeContextWithValue(activitiesKey{}, activities)
}

///////////////////////////////////////////////////////////////////////////////
/// Utility Functions
///////////////////////////////////////////////////////////////////////////////

// GetClient extracts the underlying Temporal client.Client from a broker.Broker.
// This allows advanced operations like SignalWorkflow, QueryWorkflow,
// DescribeWorkflowExecution, etc.
//
// Usage:
//
//	tc := temporal.GetClient(b)
//	tc.SignalWorkflow(ctx, "order-12345", "", "cancel-signal", nil)
func GetClient(b broker.Broker) client.Client {
	type clientAccessor interface {
		Client() client.Client
	}
	if ca, ok := b.(clientAccessor); ok {
		return ca.Client()
	}
	return nil
}
