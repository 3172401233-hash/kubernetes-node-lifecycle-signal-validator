# -*- coding: utf-8 -*-
"""节点生命周期仿真器的单元测试。

覆盖：租约延迟、关机信号、污点传播、Pod 迁移与驱逐顺序。
运行方式：python -m unittest
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from node_lifecycle import (  # noqa: E402
    PHASE_NOT_READY,
    PHASE_RUNNING,
    PHASE_SHUTDOWN_SIGNALED,
    POD_AWAITING_MIGRATION,
    POD_EVICTED,
    POD_RUNNING,
    POD_TERMINATED,
    POD_TERMINATING,
    simulate,
)


def tick_at(result, time):
    for tick in result["ticks"]:
        if tick["time"] == time:
            return tick
    raise AssertionError("时间片 %s 不存在" % time)


def pod_at(result, time, name):
    tick = tick_at(result, time)
    for pod in tick["pods"]:
        if pod["name"] == name:
            return pod
    raise AssertionError("Pod %s 不存在" % name)


class LeaseDelayTest(unittest.TestCase):
    """租约延迟：续租停止后租约到期，Ready 条件滞后更新。"""

    def test_lease_expiry_marks_not_ready_before_ready_condition(self):
        scenario = {
            "node": "node-lease",
            "tick": 5,
            "duration": 80,
            # 掉电后最后一次续租在 t=20，租约 40s，t=60 起过期。
            "lease": {"duration": 40, "renews": [0, 10, 20]},
            # Ready 条件滞后到 t=70 才变为 False。
            "ready_conditions": [[0, True], [70, False]],
            "pods": [{"name": "web", "grace_period": 5}],
        }
        result = simulate(scenario)

        self.assertEqual(tick_at(result, 55)["phase"], PHASE_RUNNING)
        self.assertTrue(tick_at(result, 55)["lease_valid"])
        # t=60 租约过期，即使 Ready 仍为 True，节点也必须判为 NotReady。
        self.assertEqual(tick_at(result, 60)["phase"], PHASE_NOT_READY)
        self.assertFalse(tick_at(result, 60)["lease_valid"])
        self.assertTrue(tick_at(result, 60)["ready"])
        # 租约过期但 Ready 滞后的窗口必须暴露 stale-ready 风险。
        self.assertTrue(
            any("stale-ready" in risk for risk in tick_at(result, 60)["risks"])
        )
        # 租约过期后不允许新增流量。
        self.assertFalse(pod_at(result, 60, "web")["accepts_traffic"])

    def test_lease_renewal_extends_validity(self):
        scenario = {
            "tick": 5,
            "duration": 45,
            "lease": {"duration": 40, "renews": [0, 30]},
            "pods": [],
        }
        result = simulate(scenario)
        # t=30 的续租使租约有效期延长到 t=70。
        self.assertEqual(tick_at(result, 45)["phase"], PHASE_RUNNING)
        self.assertTrue(tick_at(result, 45)["lease_valid"])


class ShutdownSignalTest(unittest.TestCase):
    """关机信号：NotReady 之前收到明确 shutdown，不得继续新增流量。"""

    def test_shutdown_signal_blocks_new_traffic_before_not_ready(self):
        scenario = {
            "node": "node-shutdown",
            "tick": 2,
            "duration": 40,
            "lease": {"duration": 40, "renews": [0, 10, 20, 30]},
            "ready_conditions": [[0, True], [30, False]],
            # t=10 发出短暂 shutdown 信号，此时租约未过期、Ready 仍为 True。
            "shutdown_events": [10],
            "pods": [
                {"name": "web", "grace_period": 6},
                {"name": "api", "grace_period": 12},
            ],
        }
        result = simulate(scenario)

        # 信号前：正常接收流量。
        self.assertEqual(tick_at(result, 8)["phase"], PHASE_RUNNING)
        self.assertTrue(pod_at(result, 8, "web")["accepts_traffic"])

        # 信号后、NotReady 前：进入 ShutdownSignaled，禁止新增流量。
        for time in (10, 14, 28):
            tick = tick_at(result, time)
            self.assertEqual(tick["phase"], PHASE_SHUTDOWN_SIGNALED)
            for pod in tick["pods"]:
                self.assertFalse(
                    pod["accepts_traffic"],
                    "t=%s 时 %s 不应接收新增流量" % (time, pod["name"]),
                )

        # 优雅终止时间线：web 在 t=16 终止，api 在 t=22 终止。
        self.assertEqual(pod_at(result, 12, "web")["state"], POD_TERMINATING)
        self.assertEqual(pod_at(result, 16, "web")["state"], POD_TERMINATED)
        self.assertEqual(pod_at(result, 20, "api")["state"], POD_TERMINATING)
        self.assertEqual(pod_at(result, 22, "api")["state"], POD_TERMINATED)

        # 驱逐顺序按完成时间排列。
        order = [(e["pod"], e["finished_at"]) for e in result["eviction_order"]]
        self.assertEqual(order, [("web", 16), ("api", 22)])
        self.assertEqual(sorted(result["affected_pods"]), ["api", "web"])


class TaintPropagationTest(unittest.TestCase):
    """污点传播：NotReady 到 NoExecute taint 生效之间存在驱逐空窗。"""

    def test_eviction_waits_for_taint_and_toleration(self):
        scenario = {
            "node": "node-taint",
            "tick": 5,
            "duration": 60,
            "lease": {"duration": 40, "renews": [0]},
            # t=20 节点 NotReady。
            "ready_conditions": [[0, True], [20, False]],
            # taint 传播慢，t=35 才被观察到。
            "taints": [
                {
                    "key": "node.kubernetes.io/not-ready",
                    "effect": "NoExecute",
                    "observed_at": 35,
                }
            ],
            "pods": [
                {"name": "web", "grace_period": 5, "toleration_seconds": 5},
            ],
        }
        result = simulate(scenario)

        # NotReady 与 taint 传播之间的空窗：Pod 仍在运行，必须暴露风险。
        self.assertEqual(tick_at(result, 30)["phase"], PHASE_NOT_READY)
        self.assertEqual(pod_at(result, 30, "web")["state"], POD_RUNNING)
        self.assertTrue(
            any(
                "untainted-not-ready" in risk
                for risk in tick_at(result, 30)["risks"]
            )
        )

        # 驱逐在 taint 观察时间 + 容忍时间（35+5=40）才触发。
        self.assertEqual(pod_at(result, 35, "web")["state"], POD_RUNNING)
        self.assertEqual(pod_at(result, 40, "web")["state"], "Evicting")
        self.assertEqual(pod_at(result, 45, "web")["state"], POD_EVICTED)

        order = result["eviction_order"]
        self.assertEqual(len(order), 1)
        self.assertEqual(order[0]["pod"], "web")
        self.assertEqual(order[0]["finished_at"], 45)
        self.assertEqual(order[0]["reason"], "evicted")


class PodMigrationTest(unittest.TestCase):
    """Pod 迁移：有状态 Pod 必须等待数据迁移完成才能结束驱逐。"""

    def test_stateful_pod_waits_for_migration(self):
        scenario = {
            "node": "node-migration",
            "tick": 5,
            "duration": 80,
            "lease": {"duration": 40, "renews": [0]},
            "ready_conditions": [[0, True], [20, False]],
            "taints": [
                {
                    "key": "node.kubernetes.io/not-ready",
                    "effect": "NoExecute",
                    "observed_at": 25,
                }
            ],
            "pods": [
                {"name": "cache", "grace_period": 5},
                {
                    "name": "db",
                    "stateful": True,
                    "grace_period": 10,
                    "migration": {
                        "target": "node-2",
                        "start_at": 30,
                        "duration": 25,
                    },
                },
            ],
        }
        result = simulate(scenario)

        # 无状态 cache：t=25 驱逐触发，t=30 完成。
        self.assertEqual(pod_at(result, 30, "cache")["state"], POD_EVICTED)
        # 有状态 db：优雅期 t=35 结束，但迁移 t=55 才完成，期间等待迁移。
        self.assertEqual(
            pod_at(result, 40, "db")["state"], POD_AWAITING_MIGRATION
        )
        self.assertEqual(
            pod_at(result, 50, "db")["state"], POD_AWAITING_MIGRATION
        )
        self.assertEqual(pod_at(result, 55, "db")["state"], POD_EVICTED)

        # 驱逐顺序：cache 先完成，db 等迁移完成后才结束。
        order = [(e["pod"], e["finished_at"]) for e in result["eviction_order"]]
        self.assertEqual(order, [("cache", 30), ("db", 55)])
        reasons = {e["pod"]: e["reason"] for e in result["eviction_order"]}
        self.assertEqual(reasons["db"], "evicted-after-migration")

    def test_stateful_pod_without_migration_plan_is_blocked(self):
        scenario = {
            "tick": 5,
            "duration": 40,
            "lease": {"duration": 40, "renews": [0]},
            "ready_conditions": [[0, True], [10, False]],
            "taints": [
                {
                    "key": "node.kubernetes.io/not-ready",
                    "effect": "NoExecute",
                    "observed_at": 15,
                }
            ],
            "pods": [{"name": "db", "stateful": True, "grace_period": 5}],
        }
        result = simulate(scenario)

        # 缺少迁移目标的有状态 Pod 不允许完成驱逐，并暴露风险。
        self.assertEqual(
            pod_at(result, 30, "db")["state"], POD_AWAITING_MIGRATION
        )
        self.assertTrue(
            any(
                "no-migration-plan" in risk
                for risk in tick_at(result, 30)["risks"]
            )
        )
        order = result["eviction_order"]
        self.assertEqual(len(order), 1)
        self.assertIsNone(order[0]["finished_at"])
        self.assertEqual(order[0]["reason"], "blocked-no-migration-plan")


class EvictionOrderTest(unittest.TestCase):
    """驱逐顺序：无状态先按优雅期完成，有状态等待迁移后排在最后。"""

    def test_eviction_order_sorted_by_finish_time(self):
        scenario = {
            "tick": 1,
            "duration": 60,
            "lease": {"duration": 40, "renews": [0]},
            "ready_conditions": [[0, True], [10, False]],
            "taints": [
                {
                    "key": "node.kubernetes.io/unreachable",
                    "effect": "NoExecute",
                    "observed_at": 12,
                }
            ],
            "pods": [
                {"name": "slow-web", "grace_period": 20},
                {"name": "fast-web", "grace_period": 3},
                {
                    "name": "db",
                    "stateful": True,
                    "grace_period": 5,
                    "migration": {
                        "target": "node-2",
                        "start_at": 15,
                        "duration": 30,
                    },
                },
            ],
        }
        result = simulate(scenario)
        order = [(e["pod"], e["finished_at"]) for e in result["eviction_order"]]
        self.assertEqual(
            order,
            [("fast-web", 15), ("slow-web", 32), ("db", 45)],
        )
        self.assertEqual(
            sorted(result["affected_pods"]), ["db", "fast-web", "slow-web"]
        )


if __name__ == "__main__":
    unittest.main()
