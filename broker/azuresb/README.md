# Azure Service Bus Broker

Azure Service Bus broker implementation for [go-kratos](https://github.com/go-kratos/kratos).

## 使用方式

### 创建 Broker

```go
import (
    azuresb "github.com/tx7do/kratos-transport/broker/azuresb"
)

b := azuresb.NewBroker(
    azuresb.WithConnectionString("Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>"),
)

if err := b.Connect(); err != nil {
    log.Fatal(err)
}
defer b.Disconnect()
```

### 发布消息（Queue）

```go
msg := &broker.Message{
    Headers: map[string]string{
        "source": "my-service",
    },
    Body: map[string]any{"key": "value"},
}

err := b.Publish(context.Background(), "my-queue", msg)
```

### 发布消息（Topic）

```go
err := b.Publish(context.Background(), "my-topic", msg)
```

### 订阅 Queue

```go
sub, err := b.Subscribe("my-queue",
    func(ctx context.Context, event broker.Event) error {
        fmt.Printf("received: %s\n", string(event.Message().BodyBytes()))
        return nil
    },
    nil,
)
```

### 订阅 Topic（需指定 Subscription）

```go
sub, err := b.Subscribe("my-topic",
    func(ctx context.Context, event broker.Event) error {
        fmt.Printf("received: %s\n", string(event.Message().BodyBytes()))
        return nil
    },
    nil,
    azuresb.WithSubscriptionName("my-subscription"),
)
```

### 使用 Binder 反序列化

```go
sub, err := b.Subscribe("my-queue",
    func(ctx context.Context, event broker.Event) error {
        body := event.Message().Body.(*MyMessage)
        fmt.Printf("received: %+v\n", body)
        return nil
    },
    func() any { return &MyMessage{} },
    broker.DisableAutoAck(),
)
```

### 管理操作：创建 Queue/Topic/Subscription

```go
// 创建 Queue
err := b.(*azuresb.azureBroker).EnsureQueue(ctx, "my-queue", nil)

// 创建 Topic
err := b.(*azuresb.azureBroker).EnsureTopic(ctx, "my-topic", nil)

// 创建 Subscription
err := b.(*azuresb.azureBroker).EnsureSubscription(ctx, "my-topic", "my-subscription", nil)
```

> 注：管理操作是 AzureBroker 的扩展方法，需要类型断言。如果实体已存在（409），不会报错。

### 本地开发（使用 Azurite Emulator）

```bash
# 启动 Azurite
azurite --service-bus --sb-host 127.0.0.1 --sb-port 5672
```

```go
b := azuresb.NewBroker(
    azuresb.WithConnectionString("Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true"),
)
```

## Azure Service Bus 实体模型

| 实体 | 说明 | Subscribe 行为 |
|------|------|----------------|
| **Queue** | 点对点，消息由单个消费者处理 | `Subscribe("queue-name", handler, binder)` |
| **Topic + Subscription** | 发布/订阅，每条消息投递到所有订阅 | `Subscribe("topic-name", handler, binder, WithSubscriptionName("sub-name"))` |

## 配置选项

### Broker 选项

| 选项 | 类型 | 说明 |
|------|------|------|
| `WithConnectionString(connStr)` | `string` | 连接字符串（**必填**） |

### Publish 选项

| 选项 | 类型 | 说明 |
|------|------|------|
| `WithPublishContentType(ct)` | `string` | 消息 Content-Type |
| `WithPublishSessionID(id)` | `string` | 会话 ID（需启用 Session） |
| `WithPublishMessageID(id)` | `string` | 自定义消息 ID |

### Subscribe 选项

| 选项 | 类型 | 说明 |
|------|------|------|
| `WithSubscriptionName(name)` | `string` | Topic 的 Subscription 名称 |
| `WithReceiveMode(mode)` | `azservicebus.ReceiveMode` | 接收模式（默认 PeekLock） |
