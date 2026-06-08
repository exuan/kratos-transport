# GCP Pub/Sub Broker

Google Cloud Pub/Sub broker implementation for [go-kratos](https://github.com/go-kratos/kratos).

## 使用方式

### 创建 Broker

```go
import (
    gcpubsub "github.com/tx7do/kratos-transport/broker/gcpubsub"
)

b := gcpubsub.NewBroker(
    gcpubsub.WithProjectID("my-gcp-project"),
    gcpubsub.WithCredentialsFile("/path/to/service-account-key.json"),
)

if err := b.Connect(); err != nil {
    log.Fatal(err)
}
defer b.Disconnect()
```

### 发布消息

```go
msg := &broker.Message{
    Headers: map[string]string{
        "source": "my-service",
    },
    Body: map[string]any{"key": "value"},
}

err := b.Publish(context.Background(), "my-topic", msg)
```

### 订阅消息

```go
sub, err := b.Subscribe("my-topic",
    func(ctx context.Context, event broker.Event) error {
        fmt.Printf("received: %s\n", string(event.Message().BodyBytes()))
        return nil
    },
    nil,
    gcpubsub.WithSubscriptionName("my-subscription"),
)
```

### 使用 Binder 反序列化

```go
sub, err := b.Subscribe("my-topic",
    func(ctx context.Context, event broker.Event) error {
        body := event.Message().Body.(*MyMessage)
        fmt.Printf("received: %+v\n", body)
        return nil
    },
    func() any { return &MyMessage{} },
    gcpubsub.WithSubscriptionName("my-subscription"),
    broker.DisableAutoAck(),
)
```

### 本地开发（使用 Pub/Sub Emulator）

```go
b := gcpubsub.NewBroker(
    gcpubsub.WithProjectID("test-project"),
    gcpubsub.WithEndpoint("localhost:8085"),
)
```

启动 emulator：

```bash
gcloud beta emulators pubsub start --project=test-project --host-port=localhost:8085
```

## 配置选项

### Broker 选项

| 选项 | 类型 | 说明 |
|------|------|------|
| `WithProjectID(id)` | `string` | GCP 项目 ID（**必填**） |
| `WithCredentialsFile(path)` | `string` | 服务账号密钥 JSON 文件路径 |
| `WithEndpoint(endpoint)` | `string` | 自定义 Endpoint（用于 Emulator） |

### Publish 选项

| 选项 | 类型 | 说明 |
|------|------|------|
| `WithPublishTimeout(d)` | `time.Duration` | 发布单条消息的超时时间 |
| `WithPublishOrderingKey(key)` | `string` | 消息排序键（启用消息排序） |

### Subscribe 选项

| 选项 | 类型 | 说明 |
|------|------|------|
| `WithSubscriptionName(name)` | `string` | Pub/Sub 订阅名称（默认等于 topic） |
| `WithReceiveSettings(settings)` | `pubsub.ReceiveSettings` | 接收设置（并发、批量大小等） |
