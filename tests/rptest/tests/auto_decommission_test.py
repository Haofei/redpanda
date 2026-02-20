# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierMultiProducer,
    KgoVerifierParams,
    KgoVerifierMultiConsumerGroupConsumer,
)
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.tests.prealloc_nodes import PreallocNodesTest


class AutoDecommissionTestBase(PreallocNodesTest):
    """
    Base class with shared helpers for automatic node decommissioning tests.
    """

    def __init__(self, test_context: TestContext, num_brokers: int):
        self._topics = None

        super(AutoDecommissionTestBase, self).__init__(
            test_context=test_context,
            num_brokers=num_brokers,
            node_prealloc_count=2,
        )

    def setup(self):
        # defer starting redpanda to test body
        pass

    @property
    def admin(self):
        return self.redpanda._admin

    def _create_topics(self, replication_factors: list[int] = [1, 3]):
        """
        :return: total number of partitions in all topics
        """
        total_partitions = 0
        topics: list[TopicSpec] = []
        for enumeration in range(10):
            partitions = random.randint(1, 10)
            spec = TopicSpec(
                name=f"topic-{enumeration}",
                partition_count=partitions,
                replication_factor=random.choice(replication_factors),
            )
            topics.append(spec)
            total_partitions += partitions

        rpk = RpkTool(self.redpanda)
        for spec in topics:
            rpk.create_topic(
                topic=spec.name,
                partitions=spec.partition_count,
                replicas=spec.replication_factor,
            )

        self._topics = topics

        return total_partitions

    def _surviving_nodes(self, excluded_node_id: int) -> list[ClusterNode]:
        """
        Return all started nodes except the one with the given node ID.
        """
        return [
            n
            for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) != excluded_node_id
        ]

    @property
    def msg_size(self) -> int:
        return 64

    @property
    def msg_count(self) -> int:
        # test should run for ~90s, so throughput over msg size * expected runtime should yield runtime
        return int(90 * self.producer_throughput / self.msg_size)

    @property
    def producer_throughput(self) -> int:
        # this is the total throughput for the entire producer
        return 1024

    def _get_messages_per_topic(self) -> int:
        # total messages over number of topics
        assert self._topics is not None, (
            "_topics list must be initialized by the time _get_messages_per_topic is called"
        )
        return int(self.msg_count / len(self._topics))

    def _get_throughput_per_topic(self) -> int:
        # total throughput over number of topics
        assert self._topics is not None, (
            "_topics list must be initialized by the time _get_throughput_per_topic is called"
        )
        return int(self.producer_throughput / len(self._topics))

    def _start_producer(self) -> None:
        self.redpanda.logger.info(
            f"starting kgo-verifier producer with expected runtime of {self.msg_count / self.producer_throughput}"
        )
        assert self._topics is not None, "topics must be defined to start producer"
        params = [
            KgoVerifierParams(
                topic=topic,
                msg_size=self.msg_size,
                msg_count=self._get_messages_per_topic(),
                rate_limit_bps=self._get_throughput_per_topic(),
                node=self.preallocated_nodes[0],
            )
            for topic in self._topics
        ]
        self.producer = KgoVerifierMultiProducer(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=params,
            custom_node=self.preallocated_nodes[0:1],
        )

        self.producer.start()

        self.producer.wait_for_acks([10 for _ in self._topics], 15, 1)

    def _start_consumer(self) -> None:
        assert self._topics is not None, "topics must be defined to start consumer"
        params = [
            KgoVerifierParams(
                topic=topic,
                msg_size=self.msg_size,
                msg_count=self._get_messages_per_topic(),
                node=self.preallocated_nodes[1],
            )
            for topic in self._topics
        ]
        self.consumer = KgoVerifierMultiConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topics=params,
            custom_node=self.preallocated_nodes[1:],
        )

        self.consumer.start()

    def _select_and_stop_node(self) -> tuple[int, ClusterNode, ClusterNode]:
        """
        Select a random node, stop it, and return relevant info.

        :return: (stopped_node_id, survivor_node, stopped_node)
        """
        node = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(node)
        survivor = self._surviving_nodes(node_id)[0]

        self.redpanda.logger.info(
            f"Stopping node {node_id} to trigger automatic decommissioning"
        )
        self.redpanda.stop_node(node=node)

        return node_id, survivor, node

    def verify(self):
        self.redpanda.logger.info(
            f"verifying workload: "
            f"with [rate_limit: {self.producer_throughput}, message size: {self.msg_size},"
            f" message count: {self.msg_count}]"
        )
        # let the producer and consumer finish
        self.producer.wait()
        self.consumer.wait()

    def start_redpanda(self, new_bootstrap: bool = True):
        if new_bootstrap:
            self.redpanda.set_seed_servers(self.redpanda.nodes)

        self.redpanda.start(
            auto_assign_node_id=new_bootstrap, omit_seeds_on_idx_one=not new_bootstrap
        )

    def _configure_auto_decommission(
        self,
        tick_interval_ms: int = 5000,
        unavailable_timeout_s: int = 15,
        autodecommission_timeout_s: int = 30,
    ):
        """
        Configure Redpanda for automatic node decommissioning.

        :param tick_interval_ms: Partition balancer tick interval in milliseconds
        :param unavailable_timeout_s: Node availability timeout in seconds
        :param autodecommission_timeout_s: Auto-decommission timeout in seconds
        """
        self.redpanda.add_extra_rp_conf(
            {
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec": unavailable_timeout_s,
                "partition_autobalancing_node_autodecommission_timeout_sec": autodecommission_timeout_s,
                "partition_autobalancing_tick_interval_ms": tick_interval_ms,
                "health_monitor_tick_interval": min(int(tick_interval_ms / 3), 3000),
            }
        )

    def _setup_test_environment(self, replication_factors: list[int] = [3]):
        """
        Set up the test environment by starting Redpanda, creating topics,
        and starting producer/consumer.

        :param replication_factors: List of replication factors for topic creation
        """
        self.start_redpanda(new_bootstrap=True)
        self._create_topics(replication_factors=replication_factors)
        self._start_producer()
        self._start_consumer()

    def _check_node_is_removed(self, node_id: int, survivor_node: ClusterNode) -> bool:
        """
        Check if a node has been removed from the cluster.

        :param node_id: ID of the node to check
        :param survivor_node: Active node to query for broker status
        :return: True if node is removed, False otherwise
        """
        try:
            brokers = self.admin.get_brokers(node=survivor_node)
            for b in brokers:
                if b["node_id"] == node_id:
                    return False
            return True
        except Exception as e:
            self.redpanda.logger.info(f"Error checking broker status: {e}")
            return False

    def _wait_for_node_removal(
        self, node_id: int, survivor_node: ClusterNode, wait_time_sec: int
    ):
        """
        Wait for a node to be removed from the cluster.

        :param node_id: ID of the node expected to be removed
        :param survivor_node: Active node to query for broker status
        :param wait_time_sec: Maximum time to wait in seconds
        """

        def node_is_removed():
            return self._check_node_is_removed(node_id, survivor_node)

        wait_until(
            node_is_removed,
            timeout_sec=wait_time_sec,
            backoff_sec=5,
            err_msg=f"Node {node_id} was not automatically decommissioned after {wait_time_sec} seconds",
        )

        self.redpanda.logger.info(f"Node {node_id} was successfully removed")


class AutoDecommissionTest_5(AutoDecommissionTestBase):
    """
    Auto-decommission tests on a 5-node cluster.
    """

    def __init__(self, test_context: TestContext):
        super(AutoDecommissionTest_5, self).__init__(
            test_context=test_context,
            num_brokers=5,
        )

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_automatic_node_decommissioning(self):
        """
        Test that a node is automatically decommissioned when it's unresponsive
        for the configured timeout period.
        """
        autodecommission_timeout_s = 30

        self._configure_auto_decommission(
            autodecommission_timeout_s=autodecommission_timeout_s,
        )
        self._setup_test_environment(replication_factors=[3])

        node_id, survivor_node, _ = self._select_and_stop_node()

        self._wait_for_node_removal(
            node_id, survivor_node, autodecommission_timeout_s * 4
        )

        self.verify()

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decom_timer_reset(self):
        """
        Auto decommission safety requires that the timer for auto decommission
        resets when a node restarts (if this weren't the case, a restart of a
        quorum of nodes could easily cause an early node ejection). This test
        checks that auto ejection DOES get delayed by a sufficient number of
        node restarts.
        """
        autodecommission_timeout_s = 60

        self._configure_auto_decommission(
            autodecommission_timeout_s=autodecommission_timeout_s,
        )
        self._setup_test_environment(replication_factors=[3])

        node_id, survivor_node, _ = self._select_and_stop_node()

        # Sleep for half of the auto decom timeout
        time.sleep(autodecommission_timeout_s / 2)

        # Restart the remaining nodes
        self.redpanda.restart_nodes(
            nodes=self._surviving_nodes(node_id),
            auto_assign_node_id=True,
        )

        # Wait for the controller to stabilize after restart
        self.admin.await_stable_leader("controller", namespace="redpanda")

        # Sleep for 75% of the timeout; if the timer was genuinely reset by
        # the restart, the dead node should not be eligible for decommissioning
        time.sleep(autodecommission_timeout_s * 0.75)

        assert not self._check_node_is_removed(node_id, survivor_node), (
            "a restart of the brokers should reset the autodecommission timeout"
        )

        # Wait for the remainder of the timeout to make sure it does eventually decom
        self._wait_for_node_removal(
            node_id, survivor_node, autodecommission_timeout_s * 2
        )

        self.verify()
