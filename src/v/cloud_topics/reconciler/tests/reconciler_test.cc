/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <expected>
#include <memory>
#include <utility>

using namespace cloud_topics;

namespace {

class fake_source : public reconciler::source {
public:
    fake_source(model::ntp ntp, model::topic_id_partition tidp)
      : reconciler::source(std::move(ntp), tidp) {}

    void add_batch(model::test::record_batch_spec spec) {
        if (!_source_log.empty()) {
            spec.offset = _source_log.back().last_offset() + model::offset{1};
        }
        auto batch = model::test::make_random_batch(spec);
        _source_log.push_back(std::move(batch));
    }

    kafka::offset last_reconciled_offset() override { return _lro; }

    ss::future<std::expected<void, errc>>
    set_last_reconciled_offset(kafka::offset o, ss::abort_source&) override {
        _lro = o;
        co_return std::expected<void, errc>{};
    }

    ss::future<model::record_batch_reader>
    make_reader(cloud_topic_log_reader_config cfg) override {
        chunked_vector<model::record_batch> log;
        size_t size = 0;
        for (const auto& batch : _source_log) {
            if (model::offset_cast(batch.base_offset()) < cfg.start_offset) {
                continue;
            }
            if (model::offset_cast(batch.last_offset()) > cfg.max_offset) {
                break;
            }
            size += batch.size_bytes();
            log.push_back(batch.copy());
            if (size > cfg.max_bytes) {
                break;
            }
        }
        co_return model::make_chunked_memory_record_batch_reader(
          std::move(log));
    }

private:
    kafka::offset _lro;
    chunked_vector<model::record_batch> _source_log;
};

class ReconcilerTest : public testing::Test {
public:
    ss::shared_ptr<fake_source> add_source() {
        auto ntp = model::random_ntp();
        auto tid = model::create_topic_id();
        auto src = ss::make_shared<fake_source>(
          ntp, model::topic_id_partition{tid, ntp.tp.partition});
        _reconciler.attach_source(src);
        return src;
    }

    void reconcile() { _reconciler.reconcile().get(); }

    std::optional<kafka::offset>
    metastore_next_offset(ss::shared_ptr<fake_source> src) {
        auto offsets = _metastore.get_offsets(src->topic_id_partition()).get();
        if (!offsets.has_value()) {
            return std::nullopt;
        }
        return offsets.value().next_offset;
    }

private:
    l1::fake_io _io;
    l1::simple_metastore _metastore;
    reconciler::reconciler _reconciler{&_io, &_metastore};
};

using ::testing::Optional;

} // namespace

TEST_F(ReconcilerTest, EmptySource) {
    auto src = add_source();
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);
}

TEST_F(ReconcilerTest, SingleSource) {
    auto src = add_source();
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{29});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{30}));
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{39});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{40}));
}

TEST_F(ReconcilerTest, MultipleSources) {
    auto src1 = add_source();
    auto src2 = add_source();
    auto src3 = add_source();

    src1->add_batch({.count = 10});
    src1->add_batch({.count = 5});

    src2->add_batch({.count = 20});
    src2->add_batch({.count = 15});
    src2->add_batch({.count = 10});

    src3->add_batch({.count = 8});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{14});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{44});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{7});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{15}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{45}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{8}));

    // Add more data.
    src1->add_batch({.count = 10});

    src2->add_batch({.count = 10});

    src3->add_batch({.count = 10});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{24});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{54});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{17});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{25}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{55}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{18}));

    // Add data to only one of the sources.
    src2->add_batch({.count = 10});

    reconcile();

    EXPECT_EQ(src1->last_reconciled_offset(), kafka::offset{24});
    EXPECT_EQ(src2->last_reconciled_offset(), kafka::offset{64});
    EXPECT_EQ(src3->last_reconciled_offset(), kafka::offset{17});

    EXPECT_THAT(metastore_next_offset(src1), Optional(kafka::offset{25}));
    EXPECT_THAT(metastore_next_offset(src2), Optional(kafka::offset{65}));
    EXPECT_THAT(metastore_next_offset(src3), Optional(kafka::offset{18}));
}

TEST_F(ReconcilerTest, ObjectSizeLimit) {
    auto src = add_source();

    // Total size = 50 * 3 * 512KiB = 75MiB, which is greater than the 64MiB
    // max object size.
    constexpr auto batch_count = 50;
    constexpr auto record_count = 3;
    constexpr auto record_size = 512_KiB;
    for (size_t i = 0; i < batch_count; ++i) {
        src->add_batch(
          {.count = record_count,
           .record_sizes = std::vector<size_t>(record_count, record_size)});
    }

    reconcile();

    // Check that some, but not all, data was reconciled.
    constexpr auto last_offset = batch_count * record_count - 1;
    auto lro = src->last_reconciled_offset();
    EXPECT_GT(lro, kafka::offset{0});
    EXPECT_LT(lro, kafka::offset{last_offset});

    // Reconciling again should process the rest of the data.
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{last_offset});
    EXPECT_THAT(
      metastore_next_offset(src), Optional(kafka::offset{last_offset + 1}));
}
