# Temporal

[Temporal](https://temporal.io/) 是一个开源的、云原生的分布式工作流编排引擎，用于构建可靠、可扩展的长时运行应用程序。

Temporal 通过 **Workflow（工作流）** 和 **Activity（活动）** 的抽象，将分布式编排的复杂性从业务代码中剥离，
使开发者能够以类似编写本地函数的方式编排跨服务、跨集群的分布式事务与异步任务。

## 核心概念映射

| Broker 概念  | Temporal 概念       | 说明                                     |
|------------|-------------------|----------------------------------------|
| Topic      | Task Queue        | 任务路由目标，Worker 从该队列轮询任务                    |
| Publish    | ExecuteWorkflow   | 异步启动 Workflow 执行                        |
| Request    | ExecuteWorkflow   | 同步启动 Workflow 执行，阻塞等待结果                 |
| Subscribe  | Worker + Activity | Worker 轮询 Task Queue，Activity 作为最小执行单元 |
| Message    | Workflow Input    | 序列化的消息体，作为 Workflow 入参传入                |

### Workflow / Activity / Task Queue

- **Workflow（工作流）**：整个业务主流程（例如 "下单 → 扣库存 → 支付 → 发货"），是编排逻辑的载体
- **Activity（活动）**：流程里最小执行单元（单个接口 / 函数调用），执行实际的业务操作
- **Task Queue**：任务队列，分发任务到对应的工作节点

## 支持的高级特性

| 特性                     | 说明                                       | API              |
|------------------------|------------------------------------------|------------------|
| 自定义 Workflow           | 支持注册自定义多步骤工作流                              | `WithWorkflows`  |
| 自定义 Activity          | 支持注册额外的 Activity 函数或结构体                   | `WithActivities` |
| Workflow 超时控制         | 支持 Start / Run / Execution 三级超时          | `WithStartTimeout` / `WithRunTimeout` / `WithExecutionTimeout` |
| Retry Policy           | 支持自定义重试策略（InitialInterval / MaxAttempts 等） | `WithRetryPolicy` |
| Cron 定时调度             | 支持以 Cron 表达式定时启动 Workflow                 | `WithCronSchedule` |
| Workflow ID 复用策略       | 控制 Workflow ID 冲突时的行为                     | `WithIDReusePolicy` |
| 同步等待结果                | Request 方法阻塞等待 Workflow 执行完成并返回结果，返回的 Message Headers 中包含 workflow-id 和 run-id         | `broker.Request` |
| Temporal Client 暴露     | 通过 `GetClient()` 函数获取底层 Temporal Client，支持 Signal / Query 等高级操作 | `temporal.GetClient(b)` |

## Docker 部署开发服务器

```shell
docker pull temporalio/auto-setup:latest

docker run -d \
    --name temporal-dev \
    -p 7233:7233 \
    -p 8233:8233 \
    -e DB=postgresql \
    -e DB_PORT=5432 \
    temporalio/auto-setup:latest
```

或使用 Temporal CLI 快速启动：

```shell
temporal server start-dev
```

- gRPC 端口：`7233`
- Web UI：`http://localhost:8233`
- 默认命名空间：`default`

## 使用方式

### 基础：作为消息队列使用

发布消息（启动默认 BrokerMessageWorkflow）：

```go
b := temporal.NewBroker(
    broker.WithAddress("localhost:7233"),
    temporal.WithNamespace("default"),
)
b.Init()
b.Connect()

b.Publish(ctx, "my-task-queue", broker.NewMessage([]byte(`{"hello":"world"}`)))
```

订阅消息（启动 Worker 轮询 Task Queue）：

```go
_, err := broker.Subscribe[[]byte](b, "my-task-queue",
    func(ctx context.Context, topic string, headers broker.Headers, msg *[]byte) error {
        log.Printf("received: %s", string(*msg))
        return nil
    },
)
```

### 高级：自定义 Workflow 超时与重试

```go
err := b.Publish(ctx, "order-task-queue", broker.NewMessage(orderData),
    temporal.WithWorkflowID("order-12345"),
    temporal.WithRunTimeout(10 * time.Minute),
    temporal.WithExecutionTimeout(time.Hour),
    temporal.WithRetryPolicy(&temporal.RetryPolicy{
        InitialInterval: time.Second * 5,
        BackoffCoefficient: 2.0,
        MaximumInterval: time.Minute,
        MaximumAttempts: 5,
    }),
)
```

### 高级：Cron 定时调度

```go
// 每分钟执行一次
err := b.Publish(ctx, "report-task-queue", broker.NewMessage(reportData),
    temporal.WithWorkflowID("daily-report"),
    temporal.WithCronSchedule("0 8 * * *"), // 每天早上8点
)
```

### 高级：同步等待 Workflow 结果

```go
result, err := b.Request(ctx, "my-task-queue", broker.NewMessage(inputData),
    temporal.WithWorkflowID("sync-workflow-1"),
    temporal.WithRunTimeout(time.Minute),
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("workflow result: %s\n", result.Body)
```

### 高级：注册自定义 Workflow 和 Activity

```go
// 定义自定义 Workflow
func OrderWorkflow(ctx workflow.Context, order *Order) error {
    // Step 1: 扣库存
    if err := workflow.ExecuteActivity(ctx, "DeductStock", order).Get(ctx, nil); err != nil {
        return err
    }
    // Step 2: 支付
    if err := workflow.ExecuteActivity(ctx, "ProcessPayment", order).Get(ctx, nil); err != nil {
        // 补偿：回滚库存
        _ = workflow.ExecuteActivity(ctx, "RollbackStock", order).Get(ctx, nil)
        return err
    }
    // Step 3: 发货
    return workflow.ExecuteActivity(ctx, "ShipOrder", order).Get(ctx, nil)
}

// 订阅时注册自定义 Workflow 和 Activity
_, err := b.Subscribe("order-task-queue", handler, binder,
    temporal.WithWorkflows(OrderWorkflow),
    temporal.WithActivities(DeductStock, ProcessPayment, RollbackStock, ShipOrder),
)

// 发布时指定自定义 Workflow
err := b.Publish(ctx, "order-task-queue", broker.NewMessage(orderData),
    temporal.WithWorkflowFn(OrderWorkflow),
)
```

### 高级：Signal / Query（通过底层 Client）

```go
// 获取底层 Temporal Client
tc := temporal.GetClient(b)

// 发送 Signal 到正在运行的 Workflow
err := tc.SignalWorkflow(ctx, "order-12345", "", "cancel-signal", nil)

// 查询 Workflow 状态
result, err := tc.QueryWorkflow(ctx, "order-12345", "", "order-status")
```
