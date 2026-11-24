"""节点生命周期信号验证器（离线离散时间模拟，不连接 Kubernetes）。

输入 Node Lease、Ready 条件、shutdown 事件、taint、Pod readiness、
优雅终止时间和迁移目标，逐时间片计算节点阶段、受影响 Pod 与驱逐顺序。

核心规则：
1. 租约过期或 Ready=False 时节点进入 NotReady。
2. NotReady 之前收到明确 shutdown 信号时，节点进入 ShutdownSignaled，
   所有 Pod 立即停止接收新增流量并开始优雅终止。
3. NoExecute taint 生效（观察时间 + Pod 容忍时间）后才触发驱逐。
4. 有状态 Pod 必须等待数据迁移完成后才允许驱逐结束。
"""

import json
import sys


PHASE_RUNNING = "Running"
PHASE_SHUTDOWN_SIGNALED = "ShutdownSignaled"
PHASE_NOT_READY = "NotReady"
PHASE_OFFLINE = "Offline"

POD_RUNNING = "Running"
POD_TERMINATING = "Terminating"
POD_EVICTING = "Evicting"
POD_AWAITING_MIGRATION = "AwaitingMigration"
POD_TERMINATED = "Terminated"
POD_EVICTED = "Evicted"

TERMINAL_STATES = (POD_TERMINATED, POD_EVICTED)


class PodTimeline(object):
    """单个 Pod 在整个仿真窗口内的事件时间线。"""

    def __init__(self, spec):
        self.name = spec["name"]
        self.stateful = bool(spec.get("stateful", False))
        self.ready_at = int(spec.get("ready_at", 0))
        self.grace_period = int(spec.get("grace_period", 30))
        self.toleration_seconds = int(spec.get("toleration_seconds", 0))
        migration = spec.get("migration")
        self.migration_target = None
        self.migration_end = None
        if migration:
            self.migration_target = migration.get("target")
            start_at = int(migration.get("start_at", 0))
            duration = int(migration.get("duration", 0))
            self.migration_end = start_at + duration

        # 事件时间，由 simulate() 填充
        self.terminating_at = None
        self.terminated_at = None
        self.evicting_at = None
        self.evicted_at = None
        self.eviction_reason = None

    def state_at(self, time):
        if time < self.ready_at:
            return "Pending"
        if self.evicted_at is not None and time >= self.evicted_at and (
            self.terminated_at is None or self.evicted_at <= self.terminated_at
        ):
            return POD_EVICTED
        if self.terminated_at is not None and time >= self.terminated_at:
            return POD_TERMINATED
        if self.evicting_at is not None and time >= self.evicting_at:
            if self.stateful and (
                self.migration_end is None or time < self.migration_end
            ):
                return POD_AWAITING_MIGRATION
            return POD_EVICTING
        if self.terminating_at is not None and time >= self.terminating_at:
            return POD_TERMINATING
        return POD_RUNNING


def _latest_condition(conditions, time, default=True):
    """返回 time 时刻最近一次 Ready 条件的状态。"""
    status = default
    for at, value in sorted(conditions):
        if at <= time:
            status = bool(value)
        else:
            break
    return status


def simulate(scenario):
    """运行离散时间仿真，返回每个时间片的节点状态与汇总结果。"""
    tick = int(scenario.get("tick", 1))
    duration = int(scenario.get("duration", 60))
    node_name = scenario.get("node", "node")

    lease = scenario.get("lease", {})
    lease_duration = int(lease.get("duration", 40))
    renews = sorted(int(t) for t in lease.get("renews", [0]))

    ready_conditions = [
        (int(at), bool(status))
        for at, status in scenario.get("ready_conditions", [[0, True]])
    ]
    shutdown_events = sorted(int(t) for t in scenario.get("shutdown_events", []))
    shutdown_time = shutdown_events[0] if shutdown_events else None

    taints = [
        {
            "key": t.get("key", "node.kubernetes.io/not-ready"),
            "effect": t.get("effect", "NoExecute"),
            "observed_at": int(t.get("observed_at", 0)),
        }
        for t in scenario.get("taints", [])
    ]

    pods = [PodTimeline(spec) for spec in scenario.get("pods", [])]

    def lease_valid_at(time):
        last_renew = None
        for renew in renews:
            if renew <= time:
                last_renew = renew
            else:
                break
        if last_renew is None:
            return False
        return time < last_renew + lease_duration

    def no_execute_taint_times():
        return [t["observed_at"] for t in taints if t["effect"] == "NoExecute"]

    # ---- 计算每个 Pod 的事件时间线 ----
    for pod in pods:
        # 规则 2：收到明确 shutdown 信号即开始优雅终止。
        if shutdown_time is not None and shutdown_time >= pod.ready_at:
            pod.terminating_at = shutdown_time
            pod.terminated_at = shutdown_time + pod.grace_period
            # 规则 4：有状态 Pod 的终止同样要等待数据迁移完成。
            if pod.stateful and pod.migration_end is not None:
                pod.terminated_at = max(pod.terminated_at, pod.migration_end)

        # 规则 3：NoExecute taint 生效（观察时间 + 容忍时间）后触发驱逐。
        taint_times = no_execute_taint_times()
        if taint_times:
            evicting_at = min(taint_times) + pod.toleration_seconds
            already_gone = (
                pod.terminated_at is not None and pod.terminated_at <= evicting_at
            )
            if evicting_at >= pod.ready_at and not already_gone:
                pod.evicting_at = evicting_at
                grace_end = evicting_at + pod.grace_period
                # 规则 4：有状态 Pod 必须等待数据迁移完成。
                if pod.stateful:
                    if pod.migration_end is not None:
                        pod.evicted_at = max(grace_end, pod.migration_end)
                        pod.eviction_reason = "evicted-after-migration"
                    else:
                        # 没有迁移计划的有状态 Pod 不允许完成驱逐。
                        pod.evicted_at = None
                        pod.eviction_reason = "blocked-no-migration-plan"
                else:
                    pod.evicted_at = grace_end
                    pod.eviction_reason = "evicted"

    # ---- 逐时间片采样 ----
    ticks = []
    for time in range(0, duration + 1, tick):
        lease_valid = lease_valid_at(time)
        ready = _latest_condition(ready_conditions, time)
        shutdown_received = shutdown_time is not None and time >= shutdown_time
        active_taints = [
            t["key"] for t in taints if t["observed_at"] <= time
        ]

        pod_states = []
        for pod in pods:
            state = pod.state_at(time)
            pod_states.append({"name": pod.name, "state": state})

        all_pods_done = all(
            p["state"] in TERMINAL_STATES or p["state"] == "Pending"
            for p in pod_states
        ) and any(p["state"] in TERMINAL_STATES for p in pod_states)

        if not lease_valid or not ready:
            phase = PHASE_NOT_READY
        elif shutdown_received:
            phase = PHASE_SHUTDOWN_SIGNALED
        else:
            phase = PHASE_RUNNING
        if phase == PHASE_NOT_READY and all_pods_done:
            phase = PHASE_OFFLINE

        risks = []
        if not lease_valid and ready:
            risks.append("stale-ready: 租约已过期但 Ready 条件仍为 True")
        if phase in (PHASE_NOT_READY, PHASE_OFFLINE) and not any(
            t["effect"] == "NoExecute" and t["observed_at"] <= time
            for t in taints
        ):
            risks.append("untainted-not-ready: 节点 NotReady 但 NoExecute taint 尚未传播")
        for pod, pod_state in zip(pods, pod_states):
            if (
                pod.stateful
                and pod_state["state"] == POD_AWAITING_MIGRATION
                and pod.migration_end is None
            ):
                risks.append(
                    "no-migration-plan: 有状态 Pod %s 缺少迁移目标" % pod.name
                )

        tick_pods = []
        for pod, pod_state in zip(pods, pod_states):
            # 规则 2：只有 Running 阶段且未收到 shutdown 信号才允许新增流量。
            accepts_traffic = (
                phase == PHASE_RUNNING
                and not shutdown_received
                and pod_state["state"] == POD_RUNNING
            )
            tick_pods.append(
                {
                    "name": pod.name,
                    "state": pod_state["state"],
                    "accepts_traffic": accepts_traffic,
                }
            )

        ticks.append(
            {
                "time": time,
                "phase": phase,
                "lease_valid": lease_valid,
                "ready": ready,
                "shutdown_received": shutdown_received,
                "active_taints": active_taints,
                "risks": risks,
                "pods": tick_pods,
            }
        )

    # ---- 汇总：受影响 Pod 与驱逐顺序 ----
    disruption_time = None
    candidates = [t for t in [shutdown_time] if t is not None]
    not_ready_ticks = [
        t["time"] for t in ticks if not t["lease_valid"] or not t["ready"]
    ]
    if not_ready_ticks:
        candidates.append(not_ready_ticks[0])
    if candidates:
        disruption_time = min(candidates)

    affected = []
    if disruption_time is not None:
        for pod in pods:
            if pod.ready_at <= disruption_time:
                affected.append(pod.name)

    eviction_events = []
    for pod in pods:
        finished = []
        if pod.terminated_at is not None:
            finished.append((pod.terminated_at, "terminated"))
        if pod.evicted_at is not None:
            finished.append((pod.evicted_at, pod.eviction_reason or "evicted"))
        if not finished and pod.evicting_at is not None:
            eviction_events.append(
                {
                    "pod": pod.name,
                    "finished_at": None,
                    "reason": pod.eviction_reason or "eviction-pending",
                }
            )
            continue
        for finished_at, reason in finished:
            eviction_events.append(
                {"pod": pod.name, "finished_at": finished_at, "reason": reason}
            )
    eviction_order = sorted(
        eviction_events,
        key=lambda e: (e["finished_at"] is None, e["finished_at"] or 0, e["pod"]),
    )

    return {
        "node": node_name,
        "tick": tick,
        "duration": duration,
        "ticks": ticks,
        "affected_pods": affected,
        "eviction_order": eviction_order,
    }


def format_report(result):
    """将仿真结果渲染为可读文本报告。"""
    lines = []
    lines.append(
        "节点: %s (tick=%ss, duration=%ss)"
        % (result["node"], result["tick"], result["duration"])
    )
    lines.append("")
    header = "%-6s %-18s %-7s %-6s %-9s %s" % (
        "time", "phase", "lease", "ready", "shutdown", "pods")
    lines.append(header)
    lines.append("-" * len(header))
    for tick in result["ticks"]:
        pods = ", ".join(
            "%s=%s%s" % (
                p["name"],
                p["state"],
                "" if p["accepts_traffic"] else "(no-traffic)",
            )
            for p in tick["pods"]
        )
        lines.append(
            "%-6s %-18s %-7s %-6s %-9s %s"
            % (
                tick["time"],
                tick["phase"],
                "valid" if tick["lease_valid"] else "expired",
                tick["ready"],
                tick["shutdown_received"],
                pods,
            )
        )
        for risk in tick["risks"]:
            lines.append("       ! %s" % risk)
    lines.append("")
    lines.append("受影响 Pod: %s" % (", ".join(result["affected_pods"]) or "无"))
    lines.append("驱逐顺序:")
    for index, event in enumerate(result["eviction_order"], start=1):
        finished = event["finished_at"]
        lines.append(
            "  %d. %s finished_at=%s reason=%s"
            % (
                index,
                event["pod"],
                finished if finished is not None else "pending",
                event["reason"],
            )
        )
    return "\n".join(lines)


def main(argv):
    if len(argv) != 2:
        sys.stderr.write("用法: python -m node_lifecycle <scenario.json>\n")
        return 2
    with open(argv[1], "r", encoding="utf-8") as handle:
        scenario = json.load(handle)
    result = simulate(scenario)
    print(format_report(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
