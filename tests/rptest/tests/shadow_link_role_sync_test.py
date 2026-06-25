# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Callable

from connectrpc.errors import ConnectError, ConnectErrorCode

import google.protobuf.duration_pb2 as duration_pb2
import google.protobuf.field_mask_pb2 as field_mask_pb2

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    security_pb2,
    shadow_link_pb2,
)
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.multi_cluster_services import (
    SecondaryClusterArgs,
    SecondaryClusterSpec,
    ServiceType,
)
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.tests.rbac_test_v2 import AdminV2RoleWrapper
from rptest.util import expect_timeout


# Matches roles_migrator::task_name in src/v/cluster_link/roles_migrator.h.
ROLES_MIGRATOR_TASK_NAME = "Roles Migrator Task"


def _user(name: str) -> security_pb2.RoleMember:
    """Construct a user-type RoleMember."""
    return security_pb2.RoleMember(user=security_pb2.RoleUser(name=name))


def _group(name: str) -> security_pb2.RoleMember:
    """Construct a group-type RoleMember."""
    return security_pb2.RoleMember(group=security_pb2.RoleGroup(name=name))


class RoleSyncTestBase(ShadowLinkTestBase):
    """Shared helpers for shadow-link role-sync tests. Holds no test methods so
    concrete subclasses with different topologies can reuse it without
    re-running each other's tests."""

    LINK = "role-sync-link"

    def setUp(self):
        super().setUp()
        self._dst = AdminV2RoleWrapper(AdminV2(self.target_cluster_service))

    def _create_link_with_role_sync(
        self,
        filters: list[shadow_link_pb2.NameFilter] | None = None,
        mutate_req: Callable[[shadow_link_pb2.CreateShadowLinkRequest], None]
        | None = None,
    ) -> None:
        req = self.create_default_link_request(self.LINK)
        if filters is None:
            filters = [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="synced-",
                )
            ]
        opts = shadow_link_pb2.RoleSyncOptions(
            interval=duration_pb2.Duration(seconds=1),
            paused=False,
            role_name_filters=filters,
        )
        req.shadow_link.configurations.role_sync_options.CopyFrom(opts)
        if mutate_req is not None:
            mutate_req(req)
        self.create_link_with_request(req)

    def _dst_role_members(self, role: str) -> set[str]:
        try:
            members = self._dst.get_role(role).members
        except ConnectError as e:
            assert e.code == ConnectErrorCode.NOT_FOUND, (
                f"unexpected error fetching role {role!r}: {e}"
            )
            return set()
        result: set[str] = set()
        for m in members:
            match m.WhichOneof("member"):
                case "user":
                    result.add(m.user.name)
                case "group":
                    result.add(m.group.name)
                case other:
                    raise AssertionError(
                        f"unexpected role member type {other!r} in role {role!r}"
                    )
        return result

    def _set_role_sync_paused(self, paused: bool) -> None:
        link = self.get_link(self.LINK)
        link.configurations.role_sync_options.paused = paused
        self.update_link(
            shadow_link=link,
            update_mask=field_mask_pb2.FieldMask(
                paths=["configurations.role_sync_options.paused"]
            ),
        )

    def _roles_task(self):
        for task in self.get_link(self.LINK).status.task_statuses:
            if task.name == ROLES_MIGRATOR_TASK_NAME:
                return task
        return None

    def _roles_task_state(self):
        task = self._roles_task()
        return task.state if task is not None else None


class ShadowLinkRoleSyncTest(RoleSyncTestBase):
    """End-to-end test for the shadow link roles migrator (plaintext source)."""

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(),
        )

    def setUp(self):
        super().setUp()
        self._src = AdminV2RoleWrapper(AdminV2(self.source_cluster_service))

    @cluster(num_nodes=6)
    def test_role_sync_full_mirror(self):
        """
        Verify full mirror lifecycle: initial sync of users and groups,
        membership update, out-of-scope exclusion, deletion, and the
        pause/resume cycle (including a member removal made while paused).
        """
        # Roles on the source. "synced-keep" is in scope (prefix "synced-") and
        # carries both a user and a group member; it survives to drive the
        # pause/resume sequence below. "synced-doomed" is in scope and exercises
        # deletion. "excluded-role" is out of scope.
        self._src.create_role(
            role="synced-keep", members=[_user("u1"), _group("synced-group")]
        )
        self._src.create_role(role="synced-doomed", members=[_user("u4")])
        self._src.create_role(role="excluded-role", members=[_user("u2")])

        self._create_link_with_role_sync()

        # Initial mirror: both in-scope roles, with their full membership
        # (users and groups), appear on the destination.
        def initial_mirror_complete() -> bool:
            return self._dst_role_members("synced-keep") == {
                "u1",
                "synced-group",
            } and self._dst_role_members("synced-doomed") == {"u4"}

        wait_until(
            initial_mirror_complete,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="initial role state did not mirror to destination",
        )

        # Out-of-scope role must never appear. The positive checks above prove a
        # full sync cycle completed, so excluded-role's absence here means
        # "excluded by the filter" rather than merely "not synced yet".
        assert "excluded-role" not in self._dst.list_role_names(), (
            "excluded-role (out-of-scope) should not be mirrored to destination"
        )

        # Membership addition propagates.
        self._src.add_role_members(role="synced-keep", members=[_user("u3")])
        wait_until(
            lambda: (
                self._dst_role_members("synced-keep") == {"u1", "u3", "synced-group"}
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="membership addition did not propagate",
        )

        # Role deletion propagates.
        self._src.delete_role("synced-doomed", delete_acls=False)
        wait_until(
            lambda: "synced-doomed" not in self._dst.list_role_names(),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="role deletion did not propagate",
        )

        # Pausing role sync parks the roles migrator task in the paused state.
        self._set_role_sync_paused(True)
        wait_until(
            lambda: self._roles_task_state() == shadow_link_pb2.TASK_STATE_PAUSED,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="roles migrator did not pause after role sync was paused",
        )

        # A removal made while paused must not propagate. The migrator is
        # confirmed parked above; poll for the removal across several sync
        # intervals and require that it never lands. A timeout here is the
        # success case: the forbidden state was never observed.
        self._src.remove_role_members(
            role="synced-keep", members=[_group("synced-group")]
        )
        with expect_timeout():
            wait_until(
                lambda: "synced-group" not in self._dst_role_members("synced-keep"),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Resuming the link should allow the removal to propagate.
        self._set_role_sync_paused(False)
        wait_until(
            lambda: self._roles_task_state() == shadow_link_pb2.TASK_STATE_ACTIVE,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="roles migrator did not resume after role sync was unpaused",
        )
        wait_until(
            lambda: "synced-group" not in self._dst_role_members("synced-keep"),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="member removal made while paused did not propagate on resume",
        )

    @cluster(num_nodes=6)
    def test_reconcile_authority_is_scoped(self):
        """The reconcile is full-replace authoritative, but only over in-scope
        roles. For an in-scope role the destination is driven back to the
        source: a member added directly on the destination is stripped and a
        role deleted on the destination is recreated. A destination role
        outside the filter scope, with no source counterpart, is left
        untouched."""
        self._src.create_role(
            role="synced-role", members=[_user("u1"), _group("synced-group")]
        )
        # "unmanaged-" is outside the default "synced-" include filter, so the
        # migrator never selects it on either side.
        self._dst.create_role(role="unmanaged-role", members=[_user("local-admin")])
        self._create_link_with_role_sync()
        wait_until(
            lambda: self._dst_role_members("synced-role") == {"u1", "synced-group"},
            timeout_sec=30,
            backoff_sec=1,
            err_msg="initial role state did not mirror to destination",
        )

        # A member added directly on the destination is stripped on reconcile.
        self._dst.add_role_members(role="synced-role", members=[_user("drift-member")])
        wait_until(
            lambda: self._dst_role_members("synced-role") == {"u1", "synced-group"},
            timeout_sec=30,
            backoff_sec=1,
            err_msg="foreign member added on destination was not reverted",
        )

        # A role deleted on the destination is recreated from the source.
        self._dst.delete_role("synced-role", delete_acls=False)
        wait_until(
            lambda: self._dst_role_members("synced-role") == {"u1", "synced-group"},
            timeout_sec=30,
            backoff_sec=1,
            err_msg="role deleted on destination was not recreated",
        )

        # The reverts above are observed reconcile cycles that ran while
        # "unmanaged-role" existed, so its survival here is "left alone by the
        # filter" rather than "not yet reconciled".
        assert "unmanaged-role" in self._dst.list_role_names(), (
            "out-of-scope destination role was deleted by the migrator"
        )
        assert self._dst_role_members("unmanaged-role") == {"local-admin"}, (
            "out-of-scope destination role's membership was altered by the migrator"
        )


class ShadowLinkRoleSyncKafkaSourceTest(RoleSyncTestBase):
    """Role sync degrades gracefully when the source is an Apache Kafka cluster
    that does not implement DescribeRedpandaRoles (Kafka API key 15000)."""

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(),
        )

    def get_source_cluster_spec(self) -> SecondaryClusterSpec:
        return SecondaryClusterSpec(
            ServiceType.KAFKA,
            kafka_version="3.8.0",
            kafka_quorum="COMBINED_KRAFT",
        )

    @cluster(num_nodes=6)
    def test_roles_park_but_link_still_syncs_topics(self):
        # No RBAC roles exist on an Apache Kafka source; the roles task cannot
        # negotiate the custom DescribeRedpandaRoles API (Kafka API key 15000)
        # and parks. The rest of the link rides standard Kafka APIs, so it must
        # keep working: one task parking is isolated from its siblings.
        topic = TopicSpec(name="mirror-topic", partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic)

        self._create_link_with_role_sync()

        def parked() -> bool:
            task = self._roles_task()
            return (
                task is not None
                and task.state == shadow_link_pb2.TASK_STATE_LINK_UNAVAILABLE
            )

        wait_until(
            parked,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="roles task did not park LINK_UNAVAILABLE against a Kafka source",
        )
        task = self._roles_task()
        assert task is not None and task.reason, (
            "expected a non-empty reason explaining the unsupported API"
        )
        assert task.state != shadow_link_pb2.TASK_STATE_FAULTED, (
            "roles task must park (LINK_UNAVAILABLE), not fault, on an unsupported source"
        )
        # Nothing was mirrored.
        assert not [
            n for n in self._dst.list_role_names() if n.startswith("synced-")
        ], "no roles should be mirrored from a Kafka source"

        # The parked roles task does not impair the data plane: a topic created
        # on the Kafka source still syncs to the target over standard Kafka APIs.
        wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="topic did not sync to target while the roles task was parked",
        )

        # Topic sync completing did not un-park or fault the roles task.
        assert parked(), "roles task should remain parked after topic sync"
