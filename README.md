# Modbus TCP 单连接网关（仅 FC03）

一台只允许一条 TCP 连接的 Modbus TCP 仪表，要同时服务两套巡检程序。
本仓库是一个纯 Python 网关：对外接受任意多个客户端，对内只维持一条仪表连接，
把多笔 FC03 读取并发压在同一条连接上转发，并按事务号把回包准确送回原客户端。

## 组成

| 服务 | 端口 | 作用 |
| --- | --- | --- |
| `gateway` | 5020（Modbus）/ 8081（状态 JSON） | 只转发功能码 03，单条上游连接，事务号改写与还原 |
| `meter` | 1502（Modbus）/ 8082（控制 JSON） | 模拟仪表：单连接限制、可延迟/乱序/断线 |
| `web` | 8080 | Flask 演示页：两路同时读取 + 仪表控制 + 状态面板 |

## 启动

```bash
docker compose up --build
```

打开 <http://localhost:8080>。

不用 Docker 时：

```bash
python -m pip install -r requirements.txt
python -m simulator &     # 仪表
python -m gateway &       # 网关
python -m web.app         # 页面
```

## 网关关键设计

### 单连接 + 并发在途

网关只维护一条到仪表的 TCP 连接（`gateway/core.py` 的 `_UpstreamLink`）。
请求不再串行等待：客户端帧一到就改写事务号并写入仪表 socket，回包由独立的
读循环按 MBAP 长度前缀切帧。TCP 是字节流，Modbus/TCP 的长度前缀让回包切分
与「哪笔请求先发」无关，所以仪表可以任意顺序回包。

### 事务号：改写、还原、隔离

两套巡检程序可能各自从同一个号开始编（例如都用 7）。网关在转发前把事务号
换成**当前在途请求中唯一**的内部号（`_allocate_txid`），并在 `_pending` 里记录
`内部号 → (原事务号, 原从站号, 客户端会话)`。回包按内部号查表，重写回原事务号
和原从站号后写给原会话。

- 内部号在途集合与「墓碑」集合都避开，保证不会把回包错配给新请求。
- 回包带来原样的事务号；客户端连接与请求不绑定，客户端断开不影响映射表本身。

### 超时、迟到回包、断开

- 每笔在途请求有一个 `request_timeout` 定时器。超时后：从映射表摘除、给客户端回
  Modbus 异常 `0x0B`（gateway target failed to respond），并把内部号放进墓碑
  （`late_response_quarantine`，默认 `max(30s, 2×timeout)`），期间不复用。
- 真正的迟到回包到达时映射表里已无该内部号 → 直接丢弃并计数
  `late_responses_dropped`，绝不会串到别的客户端。
- 客户端断开（或写不出去）：该会话的在途请求全部摘除并进入墓碑；之后的迟到回包
  同样丢弃。
- 仪表断线：上游 supervisor 立刻让**全部在途读取**以 `0x0B` 失败；断线期间新请求
  立即以 `0x0A`（gateway path unavailable）失败，不排队不挂起。随后自动重连
  （退避 `reconnect_delay`），重连成功后清空墓碑——旧 socket 已销毁，旧号不可能
  再有回包，因此可以安全复用。

### 只转发 FC03

其它功能码在网关本地直接回 `0x01 illegal function`，不会打到仪表；数量、
地址越界同样本地拒绝。仪表返回的 Modbus 异常原样转发（保留客户端事务号）。

### 背压与上限

- 单客户端接收缓冲上限 64 KiB，超长直接断开。
- 每客户端回包队列上限 1024 帧，写不出去说明客户端不读了，断开而不是无限缓冲。
- 全局在途上限 `max_inflight`（默认 4096），超出回 `0x06 server busy`。

## 演示页

- **通道 A / 通道 B**：地址、数量、事务号、从站号各自可改；默认「强制两路同事务号」。
  点击后两路在不同线程里同时发起，结果分栏显示读数、耗时、回包事务号。
- **仪表行为控制**：统一延迟回包、先到先回 / 后到先回、卡住下一次回包
  （`stall`）、断开 / 恢复仪表、全部复位。
- **状态面板**：网关的连接状态、在途数、成功/超时/丢弃迟到回包计数；
  仪表的设置、接入/拒绝连接数，以及仪表实际看到的最近事务号（同号并发时
  这里应显示两个不同的改写号）。

## 测试与实测

```bash
pytest -q                       # 23 个用例（codec 7 + 网关 10 + 页面 6）
python scripts/verify_stack.py  # 对已启动的栈跑三个验收场景
```

`tests/` 直接起真实的 meter + gateway（随机端口）跑异步集成测试，覆盖：
同号双客户端乱序回包、单连接多笔同号并发、超时后重试、迟到回包丢弃、
客户端断开后同号复用、仪表断线与恢复、非 03 功能码拒绝、仪表异常转发、
40 笔同号压力；`tests/test_web.py` 通过 Flask test client 跑通页面三条链路。

`scripts/verify_stack.py` 用真实 socket + HTTP 驱动黑盒栈，输出三行结论。
最近一次在本机（非 Docker）实跑：

```
[ OK ] same transaction id, reordered answers: A=[7, 10] B=[607, 610, 613]; both kept client txid 0x0077; meter exceptions/connects unchanged (1)
[ OK ] timeout, retry, late answer dropped: timeout after 2032 ms -> exception 0x0B; retry returned [37, 40]; late answer dropped (total 1)
[ OK ] meter disconnect, fail fast, recover: in-flight read failed with 0x0B; offline read failed in 1 ms with 0x0A; after recovery read returned [67, 70] (connects=2)
```

## 配置

网关环境变量：`METER_HOST`、`METER_PORT`、`GATEWAY_LISTEN_HOST/PORT`、
`GATEWAY_REQUEST_TIMEOUT`（默认 3.0）、`GATEWAY_CONNECT_TIMEOUT`（2.0）、
`GATEWAY_RECONNECT_DELAY`（0.5）、`GATEWAY_MAX_INFLIGHT`（4096）、
`GATEWAY_STATUS_PORT`（8081）、`LOG_LEVEL`。
页面：`UI_GATEWAY_HOST/PORT`、`UI_GATEWAY_STATUS_HOST/PORT`、
`UI_METER_HOST`、`UI_METER_CONTROL_PORT`、`UI_CLIENT_TIMEOUT`（默认 6）。

## 已知余项

- **迟到回包只能靠超时窗口猜**。Modbus 没有取消语义，网关不知道仪表何时放弃
  某笔请求；`late_response_quarantine` 期内不复用内部号，超过窗口后若仪表才回，
  理论上可能撞上同号新请求。默认 30 秒远大于常见仪表超时，但这不是协议保证。
- **重连后墓碑清空**依赖「旧 socket 关闭即旧回包不可能再被读到」这一事实，
  对 TCP 成立；如果将来改成多上游复用则需重新论证。
- 网关与页面都是单进程内存态：重启丢失统计；页面用 Flask 开发服务器，
  仅适合演示，生产应换 WSGI/ASGI 服务器并加认证。
- 仅实现 FC03（按需求）；没有实现 FC01/02/04、多从站网关路由、广播。
- 上游断线重连期间不接受「排队等重连」的请求，直接失败——这是为了不掩盖故障，
  若巡检程序需要缓冲，应在外层重试。
- 未做 TLS/Modbus Security，仪表与网关之间假定在可信网段。
