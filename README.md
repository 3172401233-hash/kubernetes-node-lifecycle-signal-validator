# kubernetes-node-lifecycle-signal-validator
验证节点租约、关闭信号、污点和 Pod 迁移顺序

## 背景

节点掉电前只发出短暂的 shutdown 信号，此时租约（Lease）尚未过期、
Ready 条件滞后、污点（taint）传播慢，导致部分 Pod 被驱逐，而仍在运行
的 Pod 继续接收流量。本项目用**离线离散时间仿真**复现并验证这一生命
周期，定位流量切换与驱逐顺序中的风险窗口。

## 重要说明

本工具**不连接 Kubernetes**。它不访问 apiserver、不读取真实集群状态，
仅根据输入的场景参数在本地逐时间片推演节点生命周期，用于验证信号
时序设计是否正确。

## 环境

- Python 3.8.2+（仅使用标准库）

## 输入场景（JSON）

| 字段 | 含义 |
| --- | --- |
| `node` | 节点名称 |
| `tick` / `duration` | 时间片长度（秒）/ 仿真总时长（秒） |
| `lease.duration` / `lease.renews` | 租约有效期 / 各次续租时间 |
| `ready_conditions` | Ready 条件变化序列 `[[时间, 状态], ...]` |
| `shutdown_events` | 明确关机信号时间列表 |
| `taints` | 污点（`key`/`effect`/`observed_at`，含传播延迟） |
| `pods[].grace_period` | Pod 优雅终止时间（秒） |
| `pods[].toleration_seconds` | Pod 对 NoExecute 污点的容忍时间 |
| `pods[].stateful` / `pods[].migration` | 有状态标记 / 迁移目标与起止时间 |

示例见 `examples/power_loss.json`。

## 判定规则

1. 租约过期或 Ready=False 时节点进入 `NotReady`；租约过期但 Ready 滞后
   仍为 True 时报告 `stale-ready` 风险。
2. `NotReady` 之前收到明确 shutdown 信号时，节点进入 `ShutdownSignaled`，
   所有 Pod 立即停止接收新增流量并按优雅期终止。
3. NoExecute 污点生效（观察时间 + 容忍时间）后才触发驱逐；节点
   `NotReady` 但污点尚未传播的空窗报告 `untainted-not-ready` 风险。
4. 有状态 Pod 必须等待数据迁移完成才允许终止/驱逐结束；缺少迁移目标
   的有状态 Pod 阻止驱逐并报告 `no-migration-plan` 风险。

## 输出

每个时间片的节点阶段（`Running` / `ShutdownSignaled` / `NotReady` /
`Offline`）、各 Pod 状态与是否接收新增流量、风险提示，以及汇总后的
受影响 Pod 列表和驱逐顺序。

## 使用

```bash
python -m node_lifecycle examples/power_loss.json
```

## 测试

测试覆盖租约延迟、关机信号、污点传播和 Pod 迁移：

```bash
python -m unittest
```
