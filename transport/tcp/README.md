# TCP Socket Server

Socket是网络编程的一个抽象概念。通常我们用一个Socket表示"打开了一个网络链接"，而打开一个Socket需要知道目标计算机的IP地址和端口号，再指定协议类型即可。

大多数连接都是可靠的TCP连接。创建TCP连接时，主动发起连接的叫客户端，被动响应连接的叫服务器。

举个例子，当我们在浏览器中访问新浪时，我们自己的计算机就是客户端，浏览器会主动向新浪的服务器发起连接。如果一切顺利，新浪的服务器接受了我们的连接，一个TCP连接就建立起来的，后面的通信就是发送网页内容了。

## 核心概念

### Session（会话）

每个客户端连接对应一个 Session，包含唯一的 SessionID。Server 通过 Session 管理连接生命周期。

### NetPacket（网络包）

自定义的应用层协议包格式，包含消息类型（NetMessageType）和消息体（Payload）。支持自定义序列化/反序列化。

### 消息处理

通过 `RegisterMessageHandler` 注册不同消息类型的处理器，Server 收到消息后自动路由到对应的 handler。

## 使用示例

```go
package main

import (
    "github.com/go-kratos/kratos/v2"
    "github.com/tx7do/kratos-transport/transport/tcp"
)

func main() {
    srv := tcp.NewServer(
        tcp.WithAddress("0.0.0.0:9090"),
    )

    // 注册消息处理器
    tcp.RegisterServerMessageHandler(srv, 1, func(sessionId tcp.SessionID, msg *MyMessage) error {
        log.Infof("session %s: %+v", sessionId, msg)
        return nil
    })

    app := kratos.New(
        kratos.Name("tcp-demo"),
        kratos.Server(srv),
    )

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

## 参考资料

* [TCP/IP 协议](https://zh.wikipedia.org/wiki/TCP/IP协议族)
