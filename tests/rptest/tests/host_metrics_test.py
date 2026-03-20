# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import yaml

from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest


class HostMetricsTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=1, **kwargs)

    @cluster(num_nodes=1)
    def test_basic_hoststats(self):
        """
        Test that we export the host stats

        """

        metrics = list(self.redpanda.metrics(self.redpanda.nodes[0]))

        disk_count = 0
        netstat_count = 0
        snmp_count = 0
        for family in metrics:
            if family.name == "vectorized_host_diskstats_reads":
                for sample in family.samples:
                    disk_count += int(sample.value)
            if family.name == "vectorized_host_netstat_bytes_received":
                for sample in family.samples:
                    netstat_count += int(sample.value)
            if family.name == "vectorized_host_snmp_packets_received":
                for sample in family.samples:
                    snmp_count += int(sample.value)

        assert disk_count > 0, "Expected some disk reads"
        assert netstat_count > 0, "Expected some received bytes"
        assert snmp_count > 0, "Expected some received packets"

    @cluster(num_nodes=1)
    def test_info_metric(self):
        """
        Test that the host_metrics info metric exists with exactly one sample
        per node and has the expected label values.
        """
        node = self.redpanda.nodes[0]
        metrics = list(self.redpanda.metrics(node))

        info_samples = []
        for family in metrics:
            if family.name == "vectorized_host_metrics_info":
                info_samples.extend(family.samples)

        assert len(info_samples) == 1, (
            f"Expected exactly 1 info metric sample, got {len(info_samples)}"
        )

        labels = info_samples[0].labels
        assert int(info_samples[0].value) == 1, "Info metric should be 1"

        def check(label, expected):
            actual = labels[label]
            assert actual == expected, f"Expected {label}={expected}, got {actual}"

        if self.redpanda.dedicated_nodes:
            # On dedicated nodes (EC2 etc.) device resolution should succeed.
            # Data and cache default to the same directory so they share a
            # partition.
            check("data_resolved", "1")
            check("cache_resolved", "1")
            check("same_partition", "1")
        else:
            # In docker the data directory is on an overlay filesystem whose
            # device (major 0) has no /sys/dev/block entry, so resolution
            # fails.
            check("data_resolved", "0")
            check("cache_resolved", "0")
            check("same_partition", "0")
            check("data_has_io_queue", "0")

    @cluster(num_nodes=1)
    def test_io_queue_config_metrics(self):
        """
        Test that IO queue config metrics are exported with expected labels
        and gauge values. In docker, device resolution falls back to
        directory stat, producing a single sample per gauge with empty
        device/disk labels.
        """
        node = self.redpanda.nodes[0]
        metrics = list(self.redpanda.metrics(node))

        expected_gauges = {
            "vectorized_io_queue_config_read_bytes_rate",
            "vectorized_io_queue_config_write_bytes_rate",
            "vectorized_io_queue_config_read_req_rate",
            "vectorized_io_queue_config_write_req_rate",
            "vectorized_io_queue_config_max_cost_function",
            "vectorized_io_queue_config_duplex",
        }

        expected_labels = {
            "disk",
            "device",
            "mountpoint",
            "data_disk",
            "cache_disk",
            "id",
        }

        found = {}
        for family in metrics:
            if family.name in expected_gauges:
                found[family.name] = family.samples

        assert found.keys() == expected_gauges, (
            f"Missing io_queue_config metrics: {expected_gauges - found.keys()}"
        )

        for name, samples in found.items():
            assert len(samples) >= 1, f"Expected at least 1 sample for {name}"
            for sample in samples:
                assert expected_labels.issubset(sample.labels.keys()), (
                    f"{name} missing labels: {expected_labels - sample.labels.keys()}"
                )
                # All gauges should be non-negative
                assert sample.value >= 0, f"{name} has negative value: {sample.value}"

            # At least one sample should have data_disk=1
            data_samples = [s for s in samples if s.labels["data_disk"] == "1"]
            assert len(data_samples) >= 1, (
                f"Expected at least one {name} sample with data_disk=1"
            )

        if not self.redpanda.dedicated_nodes:
            # In docker, device resolution fails so the fallback path
            # looks up IO queues by directory stat. Data and cache share
            # one overlay device, so we expect exactly 1 sample per gauge
            # with empty device/disk labels.
            def check_label(name, sample, label, expected):
                actual = sample.labels[label]
                assert actual == expected, (
                    f"{name} expected {label}={expected!r} in docker, got {actual!r}"
                )

            for name, samples in found.items():
                assert len(samples) == 1, (
                    f"{name} expected exactly 1 sample in docker, got {len(samples)}"
                )
                for label, expected in [
                    ("device", ""),
                    ("disk", ""),
                    ("mountpoint", ""),
                    ("id", "0"),
                ]:
                    check_label(name, samples[0], label, expected)

    @cluster(num_nodes=1)
    def test_io_queue_config_with_io_properties(self):
        """
        Restart Redpanda with a custom io-properties file pointing at the
        data directory. This creates a dedicated Seastar IO queue (id > 0)
        for the data dir mountpoint. We verify the configured rates appear
        in the metrics.

        On dedicated nodes with real block devices, we also expect a
        default queue (id=0) series. In docker, device resolution falls
        back to directory stat, and get_io_queue returns the configured
        queue directly, so only 1 series is emitted.
        """
        node = self.redpanda.nodes[0]

        io_props = {
            "disks": [
                {
                    "mountpoint": RedpandaService.DATA_DIR,
                    "read_iops": 10000,
                    "read_bandwidth": 1000000000,
                    "write_iops": 1000,
                    "write_bandwidth": 100000000,
                }
            ]
        }

        io_props_path = "/tmp/test-io-properties.yaml"
        node.account.create_file(io_props_path, yaml.dump(io_props))

        self.redpanda.restart_nodes(
            [node],
            extra_cli=[f"--io-properties-file={io_props_path}"],
        )

        metrics = list(self.redpanda.metrics(node))

        expected_gauges = {
            "vectorized_io_queue_config_read_bytes_rate",
            "vectorized_io_queue_config_write_bytes_rate",
            "vectorized_io_queue_config_read_req_rate",
            "vectorized_io_queue_config_write_req_rate",
            "vectorized_io_queue_config_max_cost_function",
            "vectorized_io_queue_config_duplex",
        }

        found = {}
        for family in metrics:
            if family.name in expected_gauges:
                found[family.name] = family.samples

        assert found.keys() == expected_gauges, (
            f"Missing io_queue_config metrics: {expected_gauges - found.keys()}"
        )

        # Every gauge should have at least 1 sample with the configured
        # mountpoint and the expected rate values.
        for name, samples in found.items():
            configured = [
                s for s in samples if s.labels["mountpoint"] == RedpandaService.DATA_DIR
            ]
            assert len(configured) == 1, (
                f"{name} expected 1 sample with mountpoint="
                f"{RedpandaService.DATA_DIR}, got {len(configured)}"
            )
            assert int(configured[0].labels["id"]) > 0

        def gauge_value(metric_name):
            samples = found[f"vectorized_io_queue_config_{metric_name}"]
            return [
                s for s in samples if s.labels["mountpoint"] == RedpandaService.DATA_DIR
            ][0].value

        assert gauge_value("read_bytes_rate") == 1000000000
        assert gauge_value("write_bytes_rate") == 100000000
        assert gauge_value("read_req_rate") == 10000
        assert gauge_value("write_req_rate") == 1000

        # When io-properties points at the data dir, get_io_queue for that
        # device returns the configured queue. Since data and cache share
        # the same device, they deduplicate to 1 series.
        for name, samples in found.items():
            assert len(samples) == 1, f"{name} expected 1 sample, got {len(samples)}"
