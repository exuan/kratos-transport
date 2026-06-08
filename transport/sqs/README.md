# Amazon SQS

## 什么是 Amazon SQS？

Amazon Simple Queue Service (SQS) 是 AWS 提供的全托管消息队列服务，可在软件组件之间发送、存储和接收消息，无需维护消息中间件基础设施。

SQS 提供两种队列类型：

- **标准队列**：提供无限制的吞吐量，保证至少一次投递，支持尽力而为的顺序。
- **FIFO 队列**：保证消息严格按序投递，支持精确一次处理，吞吐量上限为每秒 300 个消息（批量模式下可更高）。

主要特点：

- **无服务器**：无需预置或管理基础设施，自动扩展。
- **可靠**：消息冗余存储在多个可用区，保证高可用。
- **长轮询**：减少空响应次数，降低成本。
- **死信队列**：处理失败的消息可自动转至死信队列。
- **延迟队列**：支持消息延迟投递（0-15 分钟）。
- **消息属性**：支持自定义消息元数据。
- **FIFO 支持**：支持消息去重和有序投递。

## 核心概念

### Queue（队列）

消息的存储和转发容器。每个队列通过 Queue URL 唯一标识（如 `https://sqs.us-east-1.amazonaws.com/123456789/my-queue`）。

### Message（消息）

消息包含 Body（文本内容）和可选的 Message Attributes（结构化元数据）。单条消息最大 256 KB。

### 接收和删除

SQS 采用拉取模型。消费者调用 `ReceiveMessage` 获取消息，处理后调用 `DeleteMessage` 显式删除消息。未删除的消息在 Visibility Timeout 后重新变为可见。

### Visibility Timeout

消息被接收后对其他消费者隐藏的时间窗口（默认 30 秒）。超时后消息重新可见，可被再次消费。

## Docker部署开发环境

使用 [ElasticMQ](https://github.com/softwaremill/elasticmq)（SQS 兼容的本地模拟器）：

```shell
docker pull softwaremill/elasticmq:latest

docker run -itd \
    --name elasticmq \
    -p 9324:9324 \
    -p 9325:9325 \
    softwaremill/elasticmq:latest
```

- SQS 兼容端点：<http://localhost:9324>
- 管理界面：<http://localhost:9325>

或使用 [LocalStack](https://localstack.cloud/)：

```shell
docker pull localstack/localstack:latest

docker run -itd \
    --name localstack \
    -p 4566:4566 \
    -e SERVICES=sqs \
    localstack/localstack:latest
```

- SQS 端点：<http://localhost:4566>

## 使用示例

### 创建 Server

```go
package main

import (
    "github.com/go-kratos/kratos/v2"
    "github.com/tx7do/kratos-transport/transport/sqs"
)

func main() {
    // 连接 ElasticMQ 本地模拟器
    srv := sqs.NewServer(
        sqs.WithRegion("us-east-1"),
        sqs.WithEndpoint("http://127.0.0.1:9324"),
        sqs.WithCodec("json"),
    )

    // 连接 AWS SQS
    // srv := sqs.NewServer(
    //     sqs.WithRegion("ap-northeast-1"),
    //     sqs.WithCodec("json"),
    // )

    app := kratos.New(
        kratos.Name("sqs-demo"),
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
    sqsBroker "github.com/tx7do/kratos-transport/broker/sqs"
)

// 订阅队列（自动通过队列名解析 Queue URL）
_ = sqs.RegisterSubscriber(srv,
    "my-queue",
    func(ctx context.Context, topic string, headers broker.Headers, msg *MyMessage) error {
        log.Infof("received: %+v", msg)
        return nil
    },
)

// 显式指定 Queue URL
_ = sqs.RegisterSubscriber(srv,
    "my-queue",
    func(ctx context.Context, topic string, headers broker.Headers, msg *MyMessage) error {
        log.Infof("received: %+v", msg)
        return nil
    },
    sqsBroker.WithQueueUrl("https://sqs.us-east-1.amazonaws.com/123456789/my-queue"),
)
```

## 参考资料

* [Amazon SQS 文档](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
* [ElasticMQ - SQS 兼容的本地模拟器](https://github.com/softwaremill/elasticmq)
* [LocalStack - AWS 本地模拟器](https://localstack.cloud/)
* [AWS SDK for Go v2](https://aws.github.io/aws-sdk-go-v2/)
