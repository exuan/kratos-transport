# GCP Pub/Sub

## 什么是 Google Cloud Pub/Sub？

Google Cloud Pub/Sub 是 Google Cloud Platform 提供的全托管异步消息传递服务，专为实时事件流和消息驱动架构设计。它将消息的发送者（Publisher）和接收者（Subscriber）解耦，支持一对多、多对多的消息分发。

GCP Pub/Sub 的主要特点：

- **全球规模**：支持全球范围的消息传递，自动处理跨区域复制。
- **至少一次投递**：保证消息至少被投递一次。
- **有序消息**：支持基于 Ordering Key 的消息有序投递。
- **推/拉两种订阅模式**：支持 Pull（主动拉取）和 Push（被动推送）两种消费方式。
- **Exactly-Once 投递**（预览）：部分区域支持精确一次投递语义。
- **无服务器架构**：自动扩展，无需管理基础设施。
- **Schema Registry**：支持消息格式的集中管理和版本控制。
- **死信队列**：支持无法处理的消息自动重试和转储。

## 核心概念

### Topic（主题）

消息的目标地址。Publisher 将消息发送到 Topic。

### Subscription（订阅）

Subscription 绑定到 Topic，定义了消息的投递方式和消费者。一个 Topic 可以有多个 Subscription。

- **Pull 订阅**：消费者主动拉取消息（本模块使用此方式）。
- **Push 订阅**：Pub/Sub 将消息推送到指定的 HTTP(S) 端点。

### Message（消息）

消息由 Data（二进制数据）和 Attributes（键值对属性）组成。

### Project（项目）

GCP 资源管理的顶层容器。所有 Pub/Sub 资源都归属于某个 GCP Project。

## Docker部署开发环境

使用 [Pub/Sub Emulator](https://cloud.google.com/pubsub/docs/emulator) 进行本地开发测试：

```shell
# 安装 gcloud CLI 后启动模拟器
gcloud beta emulators pubsub start --project=test-project --host-port=0.0.0.0:8085
```

或使用 Docker：

```shell
docker pull google/cloud-sdk:latest

docker run -itd \
    --name pubsub-emulator \
    -p 8085:8085 \
    google/cloud-sdk:latest \
    gcloud beta emulators pubsub start --project=test-project --host-port=0.0.0.0:8085
```

## 使用示例

### 创建 Server

```go
package main

import (
    "github.com/go-kratos/kratos/v2"
    "github.com/tx7do/kratos-transport/transport/gcpubsub"
)

func main() {
    srv := gcpubsub.NewServer(
        gcpubsub.WithProjectID("my-gcp-project"),
        gcpubsub.WithCredentialsFile("/path/to/service-account.json"),
        gcpubsub.WithCodec("json"),
    )

    app := kratos.New(
        kratos.Name("gcpubsub-demo"),
        kratos.Server(srv),
    )

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

### 注册订阅者

```go
import (
    gcpubsubBroker "github.com/tx7do/kratos-transport/broker/gcpubsub"
)

// 订阅（Subscription 名称默认等于 Topic 名称）
_ = gcpubsub.RegisterSubscriber(srv,
    "my-topic",
    func(ctx context.Context, topic string, headers broker.Headers, msg *MyMessage) error {
        log.Infof("received: %+v", msg)
        return nil
    },
)

// 自定义 Subscription 名称
_ = gcpubsub.RegisterSubscriber(srv,
    "my-topic",
    func(ctx context.Context, topic string, headers broker.Headers, msg *MyMessage) error {
        log.Infof("received: %+v", msg)
        return nil
    },
    gcpubsubBroker.WithSubscriptionName("my-custom-subscription"),
)
```

## 参考资料

* [Google Cloud Pub/Sub 文档](https://cloud.google.com/pubsub/docs)
* [Pub/Sub Emulator 本地开发](https://cloud.google.com/pubsub/docs/emulator)
* [Pub/Sub Go 客户端库](https://cloud.google.com/pubsub/docs/reference/libraries)
