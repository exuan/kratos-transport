# Azure Service Bus

## 什么是 Azure Service Bus？

Azure Service Bus 是微软 Azure 云平台提供的全托管消息队列服务，支持企业级消息中间件功能。它实现了 AMQP 1.0 协议标准，提供可靠的消息传递、发布/订阅模式、事务支持和死信队列等特性。

Azure Service Bus 的主要特点：

- **可靠消息传递**：支持 PeekLock 和 ReceiveAndDelete 两种接收模式，保证消息至少被投递一次。
- **发布/订阅模式**：通过 Topic 和 Subscription 实现一对多的消息分发。
- **事务支持**：支持跨多个实体的原子性操作。
- **消息去重**：基于 MessageId 自动检测和丢弃重复消息。
- **会话消息**：支持消息分组和有序处理（Session-enabled Queue/Topic）。
- **死信队列**：无法处理的消息自动进入死信队列，便于排查。
- **自动转发**：支持 Queue/Subscription 之间的消息自动转发。
- **定时消息**：支持消息延迟投递（Scheduled Enqueue Time）。

## 核心概念

### Queue（队列）

点对点消息模型。多个消费者共享一个 Queue，每条消息只会被一个消费者处理。

### Topic（主题）和 Subscription（订阅）

发布/订阅模型。消息发送到 Topic，通过 Subscription 分发给多个订阅者。每个 Subscription 可以独立配置过滤规则。

### Namespace（命名空间）

Azure Service Bus 的顶层管理容器，包含 Queue、Topic 等实体。每个 Namespace 对应一个唯一的 DNS 名称（如 `your-namespace.servicebus.windows.net`）。

### 接收模式

- **PeekLock**：默认模式。消息被锁定后消费者有固定时间处理，处理完成后显式完成（Complete）或放弃（Abandon）。超时未完成则消息重新变为可用。
- **ReceiveAndDelete**：消息读取后立即从队列中删除，不保证处理成功。

## Docker部署开发环境

使用 [Azure Service Bus Emulator](https://learn.microsoft.com/en-us/azure/service-bus-messaging/test-locally-with-service-bus-emulator) 进行本地开发测试：

```shell
docker pull mcr.microsoft.com/azure-messaging/servicebus-emulator:latest

docker run -itd \
    --name servicebus-emulator \
    -p 5672:5672 \
    -p 5300:5300 \
    -e ACCEPT_EULA=Y \
    mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
```

## 使用示例

### 创建 Server

```go
package main

import (
    "github.com/go-kratos/kratos/v2"
    "github.com/tx7do/kratos-transport/transport/azuresb"
)

func main() {
    srv := azuresb.NewServer(
        azuresb.WithConnectionString("Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>"),
        azuresb.WithCodec("json"),
    )

    app := kratos.New(
        kratos.Name("azuresb-demo"),
        kratos.Server(srv),
    )

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

### 注册订阅者

```go
// 订阅 Queue
_ = azuresb.RegisterSubscriber(srv,
    "my-queue",
    func(ctx context.Context, topic string, headers broker.Headers, msg *MyMessage) error {
        log.Infof("received: %+v", msg)
        return nil
    },
)

// 订阅 Topic/Subscription
_ = azuresb.RegisterSubscriber(srv,
    "my-topic",
    func(ctx context.Context, topic string, headers broker.Headers, msg *MyMessage) error {
        log.Infof("received: %+v", msg)
        return nil
    },
    azuresbBroker.WithSubscriptionName("my-subscription"),
)
```

## 参考资料

* [Azure Service Bus 文档](https://learn.microsoft.com/zh-cn/azure/service-bus-messaging/)
* [Azure Service Bus Emulator](https://learn.microsoft.com/en-us/azure/service-bus-messaging/test-locally-with-service-bus-emulator)
* [AMQP 1.0 协议](https://www.amqp.org/)
