# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Ducktape tests that exercise the invoke_controller_reconfiguration CLI
script against a real cluster where the controller has lost quorum."""

import subprocess

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.cfr_tests.cfr_test_base import (
    MEDIUM_TIMEOUT,
    NO_LEADER,
    REALLY_LONG_TIMEOUT,
    ControllerForcedReconfigurationTestBase,
)
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

# Name of the script on PATH, installed via the script_package bazel rule.
CFR_SCRIPT = "invoke_controller_reconfiguration"

# Substring from the script's --help output used to verify correct packaging.
CFR_HELP_DESCRIPTION = \
    "Helper script to orchestrate invoking Controller Forced Reconfiguration"

# Interactive confirmation the script expects on stdin before proceeding.
CFR_CONFIRMATION_INPUT = "yes\n"

# Topic created after CFR to verify the controller is fully functional.
VALIDATION_TOPIC = "cfr-validation-topic"

# ── CFR script CLI flags ─────────────────────────────────────────────────
CFR_SUBCMD_BAREMETAL = "baremetal"
CFR_FLAG_NODES = "--nodes"
CFR_FLAG_DEAD_NODES = "--dead-nodes"
CFR_FLAG_SURVIVING_COUNT = "--surviving-count"
CFR_FLAG_HELP = "--help"


class ControllerForcedReconfigurationScriptTest(RedpandaTest):
    """
    Validates that the invoke_controller_reconfiguration helper script
    is packaged correctly and accessible on PATH in the test environment.
    """

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context=test_context, num_brokers=1)

    @cluster(num_nodes=1)
    def test_cfr_script_on_path(self) -> None:
        result = subprocess.run(
            [CFR_SCRIPT, CFR_FLAG_HELP],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, \
            f"Script exited with {result.returncode}: {result.stderr}"

        assert CFR_HELP_DESCRIPTION in result.stdout, \
            f"Expected description not found in help output:\n{result.stdout}"

        assert "baremetal" in result.stdout, \
            f"Expected 'baremetal' subcommand not found in help output:\n{result.stdout}"

        assert "kubernetes" in result.stdout, \
            f"Expected 'kubernetes' subcommand not found in help output:\n{result.stdout}"


class ControllerForcedReconfigurationBasicTest(
    ControllerForcedReconfigurationTestBase,
):
    """Exercises the invoke_controller_reconfiguration CLI script against a
    real cluster where the controller has lost quorum.

    Inherits cluster lifecycle helpers from
    ControllerForcedReconfigurationTestBase.
    """

    CLUSTER_SIZE = 3

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(
            test_context,
            cluster_size=self.CLUSTER_SIZE,
        )

        self._superuser = self.redpanda.SUPERUSER_CREDENTIALS

    # ── helpers ───────────────────────────────────────────────────────────

    def _start_redpanda(self) -> None:
        """Bootstrap the cluster."""
        self.redpanda.add_extra_rp_conf(
            {"internal_topic_replication_factor": self.cluster_size})
        self.redpanda.start()

    def _controller_recovered(self, killed_ids: list[int]) -> bool:
        """True when a controller leader exists that is NOT one of the
        killed nodes."""
        admin = self.redpanda._admin
        for node in self.redpanda.started_nodes():
            try:
                info = admin.get_partitions(
                    namespace="redpanda",
                    topic="controller",
                    partition=0,
                    node=node,
                )
                leader_id = info.get("leader_id", NO_LEADER)
                if leader_id != NO_LEADER and leader_id not in killed_ids:
                    return True
            except Exception:
                continue
        return False

    def _kill_majority_and_enter_recovery(
        self,
    ) -> tuple[list[int], list[ClusterNode]]:
        """Stop a majority of nodes and reboot the survivors into recovery
        mode.

        Returns:
            (killed_ids, survivors): the node-ids that were killed and the
            ClusterNode handles of the survivors now in recovery mode.
        """
        nodes_to_kill = self.redpanda.nodes[:self.majority_to_kill]
        survivors = self.redpanda.nodes[self.majority_to_kill:]

        killed_ids = [self.redpanda.node_id(n) for n in nodes_to_kill]
        survivor_ids = [self.redpanda.node_id(n) for n in survivors]
        self.logger.info(
            f"Killing nodes {killed_ids}, survivors: {survivor_ids}")

        for node in nodes_to_kill:
            self.redpanda.stop_node(node)

        self._bulk_toggle_recovery_mode(
            nodes=survivors,
            timeout=MEDIUM_TIMEOUT,
            recovery_mode_enabled=True,
        )

        return killed_ids, survivors

    def _run_cfr_script(self, cmd: list[str]) -> None:
        """Execute the CFR script, log its output, and assert exit code 0."""
        self.logger.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            input=CFR_CONFIRMATION_INPUT,
            capture_output=True,
            text=True,
            timeout=MEDIUM_TIMEOUT.timeout_s,
        )
        self.logger.info(f"CFR stdout:\n{result.stdout}")
        if result.stderr:
            self.logger.info(f"CFR stderr:\n{result.stderr}")

        assert result.returncode == 0, \
            f"CFR script failed (rc={result.returncode}):\n{result.stdout}\n{result.stderr}"

    def _verify_recovery(
        self,
        killed_ids: list[int],
        survivors: list[ClusterNode],
    ) -> None:
        """Wait for the controller to come back, exit recovery mode on all
        survivors, then create a topic to prove the controller is fully
        operational."""
        surviving_count = len(survivors)

        wait_until(
            lambda: self._controller_recovered(killed_ids),
            timeout_sec=REALLY_LONG_TIMEOUT.timeout_s,
            backoff_sec=REALLY_LONG_TIMEOUT.backoff_s,
            err_msg="Controller did not recover after CFR",
        )

        self._bulk_toggle_recovery_mode(
            nodes=survivors,
            timeout=MEDIUM_TIMEOUT,
            recovery_mode_enabled=False,
        )

        wait_until(
            lambda: self._controller_recovered(killed_ids),
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="Controller did not recover after exiting recovery mode",
        )

        rpk = RpkTool(
            self.redpanda,
            username=self._superuser.username,
            password=self._superuser.password,
            sasl_mechanism=self._superuser.algorithm,
        )
        rpk.create_topic(
            VALIDATION_TOPIC,
            partitions=1,
            replicas=surviving_count,
        )
        assert VALIDATION_TOPIC in rpk.list_topics()

    # ── tests ────────────────────────────────────────────────────────────

    @cluster(num_nodes=CLUSTER_SIZE)
    def test_cfr_script_manual_mode(self) -> None:
        """Run the CFR script with explicit --dead-nodes and
        --surviving-count.

        Args:
            authn: whether admin API basic-auth is required.
            tls: whether admin API TLS is enabled on the alternate listener.
        """
        is_auth_enabled = authn
        is_tls_enabled = tls

        self._start_redpanda(
            is_auth_enabled=is_auth_enabled,
            is_tls_enabled=is_tls_enabled,
        )
        killed_ids, survivors = self._kill_majority_and_enter_recovery()
        surviving_ips: list[str] = []
        for n in survivors:
            assert n.account.hostname is not None
            surviving_ips.append(n.account.hostname)
        surviving_count = len(survivors)

        cmd = [
            CFR_SCRIPT, CFR_SUBCMD_BAREMETAL,
            CFR_FLAG_NODES, *surviving_ips,
            CFR_FLAG_DEAD_NODES, *[str(i) for i in killed_ids],
            CFR_FLAG_SURVIVING_COUNT, str(surviving_count),
        ]

        self._run_cfr_script(cmd)
        self._verify_recovery(killed_ids, survivors)
