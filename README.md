# bus-redis

`bus-redis` 是 `bus` 模块的 `redis` 驱动。

## 安装

```bash
go get github.com/infrago/bus@latest
go get github.com/infrago/bus-redis@latest
```

## 接入

```go
import (
    _ "github.com/infrago/bus"
    _ "github.com/infrago/bus-redis"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[bus]
driver = "redis"
```

## 公开 API（摘自源码）

- `func (d *redisBusDriver) Connect(inst *bus.Instance) (bus.Connection, error)`
- `func (c *redisBusConnection) Register(subject string) error`
- `func (c *redisBusConnection) Open() error`
- `func (c *redisBusConnection) Close() error`
- `func (c *redisBusConnection) Start() error`
- `func (c *redisBusConnection) Stop() error`
- `func (c *redisBusConnection) Request(subject string, data []byte, timeout time.Duration) ([]byte, error)`
- `func (c *redisBusConnection) Publish(subject string, data []byte) error`
- `func (c *redisBusConnection) Enqueue(subject string, data []byte) error`
- `func (c *redisBusConnection) Stats() []infra.ServiceStats`
- `func (c *redisBusConnection) ListNodes() []infra.NodeInfo`
- `func (c *redisBusConnection) ListServices() []infra.ServiceInfo`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
