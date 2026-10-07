"""Dependency-light routing tests for both HW3 implementations.

These tests exercise the custom routing decisions without starting Ray Serve,
GPU backends, or an HTTP proxy. Minimal Ray type stubs keep the suite runnable
on a plain Python installation; the built-in A/B/C algorithms are validated at
our integration/configuration boundary, not reimplemented here.
"""

from __future__ import annotations

import ast
import importlib.util
import re
import sys
import types
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock


REPO_ROOT = Path(__file__).resolve().parents[2]
FOUR_GPU = REPO_ROOT / "HW3/ray-serve-4gpu/src/target3"
ONE_GPU = REPO_ROOT / "HW3/ray-serve-1gpu/src/target3"


@dataclass(frozen=True)
class FakeReplicaId:
    unique_id: str

    def __str__(self) -> str:
        return self.unique_id


class FakeReplica:
    def __init__(self, name: str, max_ongoing_requests: int = 4, in_flight: int = 0):
        self.replica_id = FakeReplicaId(name)
        self.max_ongoing_requests = max_ongoing_requests
        self.routing_stats = {"in_flight": in_flight}


class FakePendingRequest:
    def __init__(self, session_id: str | None = "family-1", internal_request_id: str = "req-1"):
        self.metadata = types.SimpleNamespace(
            session_id=session_id,
            internal_request_id=internal_request_id,
        )


def install_ray_stubs() -> None:
    """Install just the import-time Ray symbols used by the router modules."""
    package_names = (
        "ray",
        "ray.serve",
        "ray.serve._private",
        "ray.serve._private.request_router",
        "ray.serve.experimental",
    )
    packages = {}
    for name in package_names:
        module = types.ModuleType(name)
        module.__path__ = []
        packages[name] = module
        sys.modules[name] = module
        if "." in name:
            parent_name, child_name = name.rsplit(".", 1)
            setattr(packages.get(parent_name) or sys.modules[parent_name], child_name, module)

    common = types.ModuleType("ray.serve._private.request_router.common")
    common.PendingRequest = FakePendingRequest
    request_router = types.ModuleType("ray.serve._private.request_router.request_router")

    class FIFOMixin:
        pass

    class RequestRouter:
        pass

    request_router.FIFOMixin = FIFOMixin
    request_router.RequestRouter = RequestRouter
    replica_wrapper = types.ModuleType("ray.serve._private.request_router.replica_wrapper")
    replica_wrapper.RunningReplica = FakeReplica
    constants = types.ModuleType("ray.serve._private.constants")
    constants.SERVE_LOGGER_NAME = "test.ray.serve"

    consistent_hash = types.ModuleType(
        "ray.serve.experimental.consistent_hash_router"
    )

    class ConsistentHashRouter:
        """A controllable stand-in; hash-ring behavior itself is Ray-owned."""

        def initialize_state(self, **kwargs):
            self._deployment_id = "test-deployment"
            self._stub_base_kwargs = kwargs

        async def choose_replicas(self, candidate_replicas, pending_request=None):
            if not candidate_replicas:
                return []
            if hasattr(self, "_test_ranks"):
                return self._test_ranks
            primary_id = getattr(self, "_test_primary_id", candidate_replicas[0].replica_id)
            primary = next(r for r in candidate_replicas if r.replica_id == primary_id)
            return [[primary]]

    consistent_hash.ConsistentHashRouter = ConsistentHashRouter

    for module in (common, request_router, replica_wrapper, constants, consistent_hash):
        sys.modules[module.__name__] = module
        parent_name, child_name = module.__name__.rsplit(".", 1)
        setattr(sys.modules[parent_name], child_name, module)


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def assigned_literal(path: Path, variable: str, names: dict | None = None):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == variable for target in node.targets)
    )
    return eval(compile(ast.Expression(assignment.value), str(path), "eval"), names or {})


class RouterTestBase(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        install_ray_stubs()
        cls.four_router_module = load_module(
            "hw3_four_gpu_routers", FOUR_GPU / "routers.py"
        )
        cls.one_router_module = load_module(
            "hw3_one_gpu_routers", ONE_GPU / "routers.py"
        )

    def make_four_router(self, hot_fraction: float = 0.5):
        router = self.four_router_module.AffinityLoadAwareRouter.__new__(
            self.four_router_module.AffinityLoadAwareRouter
        )
        router.initialize_state(hot_fraction=hot_fraction)
        return router

    def make_one_router(self, **kwargs):
        router = self.one_router_module.AffinityLoadAwareRouter.__new__(
            self.one_router_module.AffinityLoadAwareRouter
        )
        router.initialize_state(**kwargs)
        router._log_fallback = lambda *args: None
        return router

    def four_gpu_primary(self, router, candidates, session_id="family-1"):
        ordered = sorted(candidates, key=lambda r: r.replica_id.unique_id)
        index = self.four_router_module._stable_index(session_id, len(ordered))
        return ordered[index]


class FourGpuRouterTests(RouterTestBase):
    async def test_affinity_is_stable_across_candidate_order_and_ignores_cooler_peers(self):
        router = self.make_four_router()
        replicas = [FakeReplica("replica-c"), FakeReplica("replica-a"), FakeReplica("replica-b")]
        request = FakePendingRequest("same-prefix-family")

        first = await router.choose_replicas(replicas, request)
        second = await router.choose_replicas(list(reversed(replicas)), request)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 1)
        self.assertEqual(len(first[0]), 1)
        self.assertIs(first[0][0], self.four_gpu_primary(router, replicas, "same-prefix-family"))

    async def test_overflow_is_inclusive_at_hot_fraction_and_offers_coolest_peer_first(self):
        router = self.make_four_router(hot_fraction=0.5)
        replicas = [FakeReplica("replica-a"), FakeReplica("replica-b"), FakeReplica("replica-c")]
        request = FakePendingRequest("hot-family")
        primary = self.four_gpu_primary(router, replicas, "hot-family")
        peers = [replica for replica in replicas if replica is not primary]
        spill = peers[0]
        coolest = peers[1]
        router._counts[primary.replica_id] = 2  # cap=4; threshold=2, inclusive
        router._counts[spill.replica_id] = 1
        router._counts[coolest.replica_id] = 0

        ranks = await router.choose_replicas(replicas, request)

        self.assertEqual(ranks, [[coolest, primary]])
        self.assertIs(ranks[0][0], coolest)
        self.assertIs(ranks[0][1], primary)

    async def test_multi_proxy_load_uses_replica_wide_count_even_when_local_count_is_nonzero(self):
        """Two proxy-local counters must not mask the shared replica queue."""
        proxy_a = self.make_four_router()
        proxy_b = self.make_four_router()
        replicas = [FakeReplica("replica-a"), FakeReplica("replica-b"), FakeReplica("replica-c")]
        request = FakePendingRequest("shared-hot-family")
        primary = self.four_gpu_primary(proxy_a, replicas, "shared-hot-family")
        other = next(replica for replica in replicas if replica is not primary)

        # Each proxy has seen one local request, while the replica reports the
        # combined in-flight count from both proxies at the threshold.
        proxy_a._counts[primary.replica_id] = 1
        proxy_b._counts[primary.replica_id] = 1
        primary.routing_stats = {"in_flight": 2}
        self.assertEqual(proxy_a._load(primary), 2)
        self.assertEqual(proxy_b._load(primary), 2)

        ranks_a = await proxy_a.choose_replicas(replicas, request)
        ranks_b = await proxy_b.choose_replicas(replicas, request)
        self.assertEqual(ranks_a, [[other, primary]])
        self.assertEqual(ranks_b, [[other, primary]])

    async def test_local_count_wins_if_replica_report_is_stale_and_hooks_floor_at_zero(self):
        router = self.make_four_router()
        replica = FakeReplica("replica-a", in_flight=1)
        router._counts[replica.replica_id] = 3
        self.assertEqual(router._load(replica), 3)

        router.on_request_completed(replica.replica_id, "request")
        self.assertEqual(router._record_in_flight(replica.replica_id), 2)
        router._counts[replica.replica_id] = 0
        router.on_request_completed(replica.replica_id, "request")
        self.assertEqual(router._record_in_flight(replica.replica_id), 0)
        router.on_request_routed(FakePendingRequest(), replica.replica_id, None)
        self.assertEqual(router._record_in_flight(replica.replica_id), 1)

    async def test_empty_and_single_replica_candidates_are_safe(self):
        router = self.make_four_router()
        self.assertEqual(await router.choose_replicas([], FakePendingRequest()), [])
        only = FakeReplica("only", max_ongoing_requests=1, in_flight=10)
        self.assertEqual(
            await router.choose_replicas([only], FakePendingRequest("one-family")),
            [[only]],
        )

    async def test_a_b_c_d_four_gpu_integration_mapping(self):
        groups = assigned_literal(
            FOUR_GPU / "deploy_app.py",
            "GROUP_CONFIG",
            {"AffinityLoadAwareRouter": self.four_router_module.AffinityLoadAwareRouter},
        )
        for group in ("A", "B1", "B2"):
            self.assertEqual(groups[group], {"router": "default", "router_kwargs": {}})
        self.assertEqual(
            groups["C"],
            {
                "router": "ray.serve.experimental.consistent_hash_router.ConsistentHashRouter",
                "router_kwargs": {"num_fallback_replicas": 0},
            },
        )
        self.assertIs(groups["D"]["router"], self.four_router_module.AffinityLoadAwareRouter)
        self.assertEqual(groups["D"]["router_kwargs"], {"hot_fraction": 0.5})

        run_group = (FOUR_GPU / "run_group.sh").read_text(encoding="utf-8")
        route_rows = dict(
            (group, (router.strip(), max_ongoing.strip()))
            for group, router, max_ongoing in re.findall(
                r"^\s*(A|B1|B2|C|D)\)\s+ROUTER=([^;]+);\s+MQR=(.*);;",
                run_group,
                flags=re.MULTILINE,
            )
        )
        self.assertEqual(
            route_rows,
            {
                "A": ("p2c", "5"),
                "B1": ("p2c", "16"),
                "B2": ("p2c", "32"),
                "C": ("consistent_hash", '"${B_MAX_ONGOING:-16}"'),
                "D": ("affinity_load", '"${B_MAX_ONGOING:-16}"'),
            },
        )


class OneGpuRouterTests(RouterTestBase):
    async def test_overflow_precedes_affinity_rank_at_threshold(self):
        router = self.make_one_router(fallback_inflight_threshold=3)
        primary, spill = FakeReplica("primary"), FakeReplica("spill")
        router._test_primary_id = primary.replica_id
        router._probe_queue_lens = AsyncMock(return_value=[(primary, 3), (spill, 0)])

        ranks = await router.choose_replicas(
            [primary, spill], FakePendingRequest("family")
        )

        self.assertEqual(ranks, [[spill], [primary]])
        router._probe_queue_lens.assert_awaited_once()

    async def test_under_threshold_or_unknown_primary_queue_keeps_affinity(self):
        primary, spill = FakeReplica("primary"), FakeReplica("spill")
        request = FakePendingRequest("family")

        under = self.make_one_router(fallback_inflight_threshold=3)
        under._test_primary_id = primary.replica_id
        under._probe_queue_lens = AsyncMock(return_value=[(primary, 2), (spill, 0)])
        self.assertEqual(await under.choose_replicas([primary, spill], request), [[primary]])

        unknown = self.make_one_router(fallback_inflight_threshold=3)
        unknown._test_primary_id = primary.replica_id
        unknown._probe_queue_lens = AsyncMock(return_value=[(spill, 0)])
        self.assertEqual(await unknown.choose_replicas([primary, spill], request), [[primary]])

    async def test_independent_proxy_routers_probe_global_queue_and_choose_same_spill(self):
        primary, spill, peer = (
            FakeReplica("primary"),
            FakeReplica("spill"),
            FakeReplica("peer"),
        )
        current_queue_snapshot = [(primary, 4), (spill, 1), (peer, 2)]
        routers = [self.make_one_router(fallback_inflight_threshold=4) for _ in range(2)]
        for router in routers:
            router._test_primary_id = primary.replica_id
            router._probe_queue_lens = AsyncMock(return_value=current_queue_snapshot)

        selections = [
            await router.choose_replicas(
                [primary, spill, peer], FakePendingRequest("hot-family")
            )
            for router in routers
        ]

        self.assertEqual(selections, [[[spill], [primary]], [[spill], [primary]]])
        for router in routers:
            router._probe_queue_lens.assert_awaited_once()

    async def test_default_threshold_is_seventy_five_percent_with_minimum_one(self):
        router = self.make_one_router()
        self.assertEqual(router._threshold_for(FakeReplica("cap-4", max_ongoing_requests=4)), 3)
        self.assertEqual(router._threshold_for(FakeReplica("cap-1", max_ongoing_requests=1)), 1)

    async def test_a_b_c_d_one_gpu_mode_mapping(self):
        modes = assigned_literal(ONE_GPU / "serve_app.py", "ROUTER_MODES")
        self.assertEqual(modes["A_default"]["router"], "p2c")
        self.assertEqual(modes["B_cand1"]["router"], "p2c")
        self.assertEqual(modes["B_cand2"]["router"], "p2c")
        self.assertEqual(modes["C_affinity"]["router"], "consistent_hash")
        self.assertEqual(modes["D_improved"]["router"], "custom")


if __name__ == "__main__":
    unittest.main()
