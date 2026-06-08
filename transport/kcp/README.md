# KCP Server

## 介绍

KCP 是一个基于 UDP 的快速可靠传输协议，由 skywind3000 开发。相比 TCP，KCP牺牲了约 10%~20% 的带宽，但能显著降低延迟（降低 30%~40% 的平均延迟，最大延迟降低 3 倍），非常适合实时对战游戏、音视频通话等对延迟敏感的场景。

KCP 的核心特点：

- **低延迟**：通过快速重传和选择性重传大幅降低延迟
- **可靠传输**：在 UDP 之上实现可靠交付
- **ARQ 协议**：自动重传请求，保证数据完整有序到达
- **拥塞控制可选**：可关闭拥塞控制以获得更低延迟
- **前向纠错**：支持 Reed-Solomon 编码的 FEC（前向纠错），通过数据冗余抵抗丢包

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
    "github.com/tx7do/kratos-transport/transport/kcp"
)

func main() {
    srv := kcp.NewServer(
        kcp.WithAddress("0.0.0.0:9090"),
    )

    // 注册消息处理器
    kcp.RegisterServerMessageHandler(srv, 1, func(sessionId kcp.SessionID, msg *MyMessage) error {
        log.Infof("session %s: %+v", sessionId, msg)
        return nil
    })

    app := kratos.New(
        kratos.Name("kcp-demo"),
        kratos.Server(srv),
    )

    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

## 参考资料

* [KCP 协议](https://github.com/skywind3000/kcp)
* [go-kcp](https://github.com/xtaci/kcp-go)
