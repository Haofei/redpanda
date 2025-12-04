/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "gtest/gtest.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/impl.h"
#include "lsm/io/memory_persistence.h"
#include "random/generators.h"
#include "test_utils/async.h"

#include <gtest/gtest.h>

#include <memory>

namespace {

namespace io = lsm::io;
using lsm::internal::file_handle;

class proxy_persistence
  : public io::data_persistence
  , public io::metadata_persistence {
public:
    explicit proxy_persistence(
      io::data_persistence* p, io::metadata_persistence* mdp)
      : _p(p)
      , _mdp(mdp) {}

    ss::future<io::optional_pointer<io::random_access_file_reader>>
    open_random_access_reader(file_handle h) override {
        return _p->open_random_access_reader(h);
    }

    ss::future<std::unique_ptr<io::sequential_file_writer>>
    open_sequential_writer(file_handle h) override {
        return _p->open_sequential_writer(h);
    }

    ss::future<> remove_file(file_handle h) override {
        return _p->remove_file(h);
    }

    ss::coroutine::experimental::generator<file_handle> list_files() override {
        return _p->list_files();
    }

    ss::future<std::optional<iobuf>>
    read_manifest(lsm::internal::database_epoch e) override {
        return _mdp->read_manifest(e);
    }

    // Write the manifest atomically to durable storage.
    ss::future<>
    write_manifest(lsm::internal::database_epoch e, iobuf b) override {
        return _mdp->write_manifest(e, std::move(b));
    }

    ss::future<> close() override { co_return; };

private:
    io::data_persistence* _p;
    io::metadata_persistence* _mdp;
};

class ImplTest : public testing::Test {
public:
    void SetUp() override {
        _options = ss::make_lw_shared<lsm::internal::options>({
          .level_zero_slowdown_writes_trigger = 4,
          .level_zero_stop_writes_trigger = 6,
          .write_buffer_size = 256_KiB,
          .level_one_compaction_trigger = 2,
          .max_file_size = 2_MiB,
        });
        _data_persistence = lsm::io::make_memory_data_persistence();
        _meta_persistence = lsm::io::make_memory_metadata_persistence();
        _db = lsm::db::impl::open(
                _options,
                {
                  .data = std::make_unique<proxy_persistence>(
                    _data_persistence.get(), _meta_persistence.get()),
                  .metadata = std::make_unique<proxy_persistence>(
                    _data_persistence.get(), _meta_persistence.get()),
                })
                .get();
    }

    void TearDown() override {
        _db->close().get();
        _data_persistence->close().get();
        _meta_persistence->close().get();
    }

    void write_at_least(size_t size) {
        auto batch = ss::make_lw_shared<lsm::db::memtable>();
        auto seqno = _db->max_applied_seqno();
        while (batch->approximate_memory_usage() < size) {
            auto key = lsm::internal::key::encode({
              .key = random_generators::gen_alphanum_max_distinct(100'000),
              .seqno = ++seqno,
            });
            auto value = iobuf::from(
              random_generators::gen_alphanum_string(1_KiB));
            _shadow.insert_or_assign(
              ss::sstring(key.user_key()), value.share());
            batch->put(key, value.share());
        }
        _db->apply(std::move(batch)).get();
    }

    testing::AssertionResult matches_shadow() {
        auto iter = _db->create_iterator(std::nullopt).get();
        auto it = _shadow.begin();
        std::vector<std::string> errors;
        for (iter->seek_to_first().get(); iter->valid(); iter->next().get()) {
            if (it == _shadow.end()) {
                errors.emplace_back("extra elements");
                break;
            }
            if (*it != std::make_pair(iter->key().user_key(), iter->value())) {
                errors.push_back(
                  fmt::format(
                    "expected key {}, got key {}, keys equal {}, values equal "
                    "{}",
                    it->first,
                    iter->key(),
                    it->first == iter->key().user_key(),
                    it->second == iter->value()));
            }
            ++it;
        }
        if (it != _shadow.end()) {
            errors.emplace_back("missing elements");
        }
        if (errors.empty()) {
            return testing::AssertionSuccess();
        }
        return testing::AssertionFailure()
               << fmt::format("{}", fmt::join(errors, "\n"));
    }

    void restart() {
        _db->close().get();
        _db = lsm::db::impl::open(
                _options,
                {
                  .data = std::make_unique<proxy_persistence>(
                    _data_persistence.get(), _meta_persistence.get()),
                  .metadata = std::make_unique<proxy_persistence>(
                    _data_persistence.get(), _meta_persistence.get()),
                })
                .get();
    }

    auto max_applied_seqno() { return _db->max_applied_seqno(); }
    auto max_persisted_seqno() { return _db->max_persisted_seqno(); }

    ss::future<std::vector<file_handle>> list_files() {
        auto gen = _data_persistence->list_files();
        std::vector<file_handle> files;
        while (auto file = co_await gen()) {
            files.push_back(*file);
        }
        co_return files;
    }

protected:
    std::map<ss::sstring, iobuf> _shadow;
    ss::lw_shared_ptr<lsm::internal::options> _options;
    std::unique_ptr<lsm::io::data_persistence> _data_persistence;
    std::unique_ptr<lsm::io::metadata_persistence> _meta_persistence;
    std::unique_ptr<lsm::db::impl> _db;
};

TEST_F(ImplTest, MemtableIsFlushed) {
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    write_at_least(256_KiB);
    EXPECT_TRUE(matches_shadow());
    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        return list_files().then(
          [](const auto& files) { return files.size() > 0; });
    });
}

TEST_F(ImplTest, Recovery) {
    write_at_least(512_KiB);
    write_at_least(512_KiB);
    EXPECT_TRUE(matches_shadow());
    tests::drain_task_queue().get();
    _db->flush().get();
    EXPECT_EQ(max_applied_seqno(), max_persisted_seqno());
    restart();
    EXPECT_TRUE(matches_shadow());
}

TEST_F(ImplTest, Randomized) {
#ifndef NDEBUG
    int rounds = 100;
#else
    int rounds = 1000;
#endif
    for (int i = 0; i < rounds; ++i) {
        write_at_least(512_KiB);
        EXPECT_TRUE(matches_shadow());
    }
}

TEST_F(ImplTest, ReadYourOwnWrites) {
    // Write some initial data to the database
    auto batch1 = ss::make_lw_shared<lsm::db::memtable>();
    auto seqno = _db->max_applied_seqno();
    auto key1 = lsm::internal::key::encode({
      .key = "key1",
      .seqno = ++seqno,
    });
    auto value1 = iobuf::from("value1");
    batch1->put(key1, value1.share());

    auto key2 = lsm::internal::key::encode({
      .key = "key2",
      .seqno = ++seqno,
    });
    auto value2 = iobuf::from("value2");
    batch1->put(key2, value2.share());

    _db->apply(batch1).get();

    // Create a pending write batch with new keys (not yet applied)
    auto pending_batch = ss::make_lw_shared<lsm::db::memtable>();
    seqno = _db->max_applied_seqno();

    auto key3 = lsm::internal::key::encode({
      .key = "key3",
      .seqno = ++seqno,
    });
    auto value3 = iobuf::from("value3");
    pending_batch->put(key3, value3.share());

    // Verify new key is visible through iterator with write batch
    auto iter = _db->create_iterator(pending_batch).get();
    std::map<ss::sstring, iobuf> seen;
    for (iter->seek_to_first().get(); iter->valid(); iter->next().get()) {
        seen.insert_or_assign(
          ss::sstring(iter->key().user_key()), iter->value());
    }
    EXPECT_EQ(seen.size(), 3);
    EXPECT_EQ(seen["key1"], value1);
    EXPECT_EQ(seen["key2"], value2);
    EXPECT_EQ(seen["key3"], value3);

    // Create another pending write batch that updates an existing key
    auto update_batch = ss::make_lw_shared<lsm::db::memtable>();
    seqno = _db->max_applied_seqno();

    auto key2_updated = lsm::internal::key::encode({
      .key = "key2",
      .seqno = ++seqno,
    });
    auto value2_updated = iobuf::from("value2_updated");
    update_batch->put(key2_updated, value2_updated.share());

    // Verify updated value is visible through iterator with write batch
    auto iter2 = _db->create_iterator(update_batch).get();
    seen.clear();
    for (iter2->seek_to_first().get(); iter2->valid(); iter2->next().get()) {
        seen.insert_or_assign(
          ss::sstring(iter2->key().user_key()), iter2->value());
    }
    EXPECT_EQ(seen.size(), 2);
    EXPECT_EQ(seen["key1"], value1);
    EXPECT_EQ(seen["key2"], value2_updated);

    // Apply the update batch and verify all data is now committed
    _db->apply(update_batch).get();

    auto iter3 = _db->create_iterator(std::nullopt).get();
    seen.clear();
    for (iter3->seek_to_first().get(); iter3->valid(); iter3->next().get()) {
        seen.insert_or_assign(
          ss::sstring(iter3->key().user_key()), iter3->value());
    }
    EXPECT_EQ(seen.size(), 2);
    EXPECT_EQ(seen["key1"], value1);
    EXPECT_EQ(seen["key2"], value2_updated);
}

} // namespace
