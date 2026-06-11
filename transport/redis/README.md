# Redis

Redis 是一个开源（BSD许可）的内存数据结构存储系统，可以用作数据库、缓存和消息代理。本模块支持使用 Redis 的 Pub/Sub 和 Stream 两种模式作为消息传输通道。

## 双驱动模式

本模块支持两种 Redis 消息驱动：

| 驱动 | 说明 | 适用场景 |
|------|------|----------|
| **PubSub** | 基于 Redis Publish/Subscribe 命令 | 简单的实时消息广播，消息不持久化 |
| **Stream** | 基于 Redis Stream（XREADGROUP/XADD） | 需要消息持久化、消费组、消息确认的场景 |

默认使用 PubSub 模式。

## 核心概念

### PubSub 模式

基于 Redis 原生的 Publish/Subscribe 机制：
- 发布者向 Channel 发送消息
- 所有订阅了该 Channel 的订阅者同时收到消息
- 消息不持久化，离线订阅者无法收到历史消息

### Stream 模式

基于 Redis 5.0+ 引入的 Stream 数据类型：
- 支持消费组（Consumer Group），多个消费者可以分担同一个 Stream 的消息
- 消息持久化，支持消息确认（ACK）和重新投递
- 支持从任意位置开始消费
- 类似于 Kafka 的消费模型

## Docker部署开发环境

```shell
docker pull soldevelo/redis:latest
docker pull bitnami/redis-exporter:latest

docker run -itd \
    --name redis-test \
    -p 6379:6379 \
    -e ALLOW_EMPTY_PASSWORD=yes \
    soldevelo/redis:latest
```

管理工具：

- [RedisInsight](https://redis.io/insight/)
- [Another Redis Desktop Manager](https://github.com/qishibo/AnotherRedisDesktopManager)

## 使用示例

### 创建 Server（PubSub 模式，默认）

```go
package main

import (
    "github.com/go-kratos/kratos/v2"
    "github.com/tx7do/kratos-transport/transport/redis"
)

func main() {
    srv := redis.NewServer(
        redis.WithAddress("redis://127.0.0.1:6379/0"),
        redis.WithCodec("json"),
    )

    app := kratos.New(
        kratos.Name("redis-demo"),
        kratos.Server(srv),
    )

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

### 创建 Server（Stream 模式）

```go
import (
    redisBroker "github.com/tx7do/kratos-transport/broker/redis/option"
)

srv := redis.NewServer(
    redis.WithAddress("redis://127.0.0.1:6379/0"),
    redis.WithCodec("json"),
    redis.WithDriverType(redisBroker.DriverTypeStream),
)
```

### 注册订阅者

```go
_ = redis.RegisterSubscriber(srv,
    "my-channel",
    func(ctx context.Context, topic string, headers broker.Headers, msg *MyMessage) error {
        log.Infof("received: %+v", msg)
        return nil
    },
)
```

## 参考资料

* [Redis 官方文档](https://redis.io/documentation)
* [Redis Pub/Sub](https://redis.io/docs/manual/pubsub/)
* [Redis Streams](https://redis.io/docs/data-types/streams/)
* [Redis Stream 详解](https://redis.io/topics/streams-intro)