# SQS

基于 [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2) 实现的 Amazon SQS 消息代理。

Amazon Simple Queue Service (Amazon SQS) 是一项 Web 服务，让您能够访问存储待处理消息的消息队列。借助 Amazon SQS，您能够快速构建可在任何计算机上运行的消息队列应用程序。

Amazon SQS 可以提供可靠、安全并且高度可扩展的托管队列服务，用于存储在计算机之间传输的消息。借助 Amazon SQS，您可以在不同的分布式应用程序组件之间移动数据，同时既不会丢失消息，也不需要各个组件始终处于可用状态。

## 队列类型

| 队列类型 | 说明 |
|----------|------|
| **标准队列** | 默认队列类型，每秒处理近乎无限数量的事务，至少一次投递，最大努力排序 |
| **FIFO 队列** | 严格先入先出，消息只传送一次，支持消息组，上限 300 TPS |

## 使用方式

### 基础：发布/订阅

```go
b := sqs.NewBroker(
    broker.WithAddress("http://127.0.0.1:9324"),
    sqs.WithRegion("elasticmq"),
    sqs.WithEndpoint("http://127.0.0.1:9324"),
    sqs.WithQueueUrl("http://127.0.0.1:9324/queue/test-queue"),
    broker.WithCodec("json"),
)
b.Init()
b.Connect()
defer b.Disconnect()

// 发布
b.Publish(ctx, "test-queue", broker.NewMessage(msg))

// 订阅（长轮询，自动删除消息）
sub, _ := b.Subscribe("test-queue", handler, binder)
```

### 高级：FIFO 队列

```go
b.Publish(ctx, "my-fifo-queue.fifo", broker.NewMessage(msg),
    sqs.WithMessageGroupId("order-group-1"),
    sqs.WithMessageDeduplicationId("order-12345"),
)
```

### 高级：延迟投递 + 可见性超时

```go
// 延迟 60 秒投递
b.Publish(ctx, "test-queue", broker.NewMessage(msg),
    sqs.WithDelaySeconds(60),
)

// 订阅时自定义可见性超时和长轮询参数
sub, _ := b.Subscribe("test-queue", handler, binder,
    sqs.WithVisibilityTimeout(60),
    sqs.WithWaitTimeSeconds(20),
    sqs.WithMaxMessages(10),
)
```

## 配置选项

### Broker 选项

| 选项 | 说明 |
|------|------|
| `sqs.WithRegion(region)` | AWS 区域（默认 `us-east-1`） |
| `sqs.WithEndpoint(url)` | 自定义 Endpoint（用于 ElasticMQ/LocalStack 本地测试） |
| `sqs.WithQueueUrl(url)` | 默认队列 URL |

### Publish 选项

| 选项 | 说明 |
|------|------|
| `sqs.WithDelaySeconds(seconds)` | 延迟投递秒数（0-900） |
| `sqs.WithMessageGroupId(groupId)` | FIFO 队列消息组 ID |
| `sqs.WithMessageDeduplicationId(dedupId)` | FIFO 队列去重 ID |

### Subscribe 选项

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `sqs.WithVisibilityTimeout(seconds)` | 消息可见性超时 | 30 |
| `sqs.WithWaitTimeSeconds(seconds)` | 长轮询等待时间 | 20 |
| `sqs.WithMaxMessages(n)` | 每次轮询最大消息数 | 10 |

## Docker 部署开发服务器

Alpine SQS 是第三方提供的开源实现，基于 ElasticMQ，与 AWS API 兼容：

```shell
docker pull roribio16/alpine-sqs:latest

docker run -d \
      --name sqs \
      -p 9324:9324 \
      -p 9325:9325 \
      roribio16/alpine-sqs:latest
```

- SQS API 端口：`9324`
- 管理界面：`http://localhost:9325`

## 参考资料

- [Amazon SQS 产品详情](https://aws.amazon.com/cn/sqs/details/?nc1=h_ls)
- [用 Docker 跑 SQS 範例隨記](https://lihan.cc/2021/11/1131/)
