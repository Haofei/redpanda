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

#include "lsm/lsm.h"

#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/impl.h"
#include "lsm/db/memtable.h"

#include <seastar/core/coroutine.hh>

#include <stdexcept>

namespace lsm {

namespace {

ss::lw_shared_ptr<internal::options> translate_options(options opts) {
    if (opts.num_levels < 2) {
        throw std::invalid_argument(
          fmt::format(
            "num_levels must be at least 2, got {}", opts.num_levels));
    }

    if (
      opts.level_zero_stop_writes_trigger
      <= opts.level_zero_slowdown_writes_trigger) {
        throw std::invalid_argument(
          fmt::format(
            "level_zero_stop_writes_trigger ({}) must be greater than "
            "level_zero_slowdown_writes_trigger ({})",
            opts.level_zero_stop_writes_trigger,
            opts.level_zero_slowdown_writes_trigger));
    }

    if (
      opts.sst_filter_period != 0
      && (opts.sst_filter_period & (opts.sst_filter_period - 1)) != 0) {
        throw std::invalid_argument(
          fmt::format(
            "sst_filter_period must be a power of two, got {}",
            opts.sst_filter_period));
    }

    if (
      opts.level_one_compaction_trigger
      >= opts.level_zero_slowdown_writes_trigger) {
        throw std::invalid_argument(
          fmt::format(
            "level_one_compaction_trigger ({}) must be less than "
            "level_zero_slowdown_writes_trigger ({})",
            opts.level_one_compaction_trigger,
            opts.level_zero_slowdown_writes_trigger));
    }

    // Create the internal options
    auto internal_opts = ss::make_lw_shared<internal::options>();

    // Set database epoch
    internal_opts->database_epoch = internal::database_epoch{
      opts.database_epoch};

    // Create level configs based on num_levels
    internal_opts->levels.clear();
    internal_opts->levels.reserve(opts.num_levels);
    for (uint8_t i = 0; i < opts.num_levels; ++i) {
        internal_opts->levels.emplace_back(internal::level{i});
    }

    internal_opts->level_zero_slowdown_writes_trigger
      = opts.level_zero_slowdown_writes_trigger;
    internal_opts->level_zero_stop_writes_trigger
      = opts.level_zero_stop_writes_trigger;
    internal_opts->write_buffer_size = opts.write_buffer_size;
    internal_opts->level_one_compaction_trigger
      = opts.level_one_compaction_trigger;
    internal_opts->max_file_size = opts.max_file_size;
    internal_opts->max_open_files = opts.max_open_files;
    internal_opts->block_cache_size = opts.block_cache_size;
    internal_opts->sst_block_size = opts.sst_block_size;
    internal_opts->sst_filter_period = opts.sst_filter_period;

    switch (opts.compression) {
    case options::compression_type::none:
        internal_opts->compression = compression_type::none;
        break;
    case options::compression_type::zstd:
        internal_opts->compression = compression_type::zstd;
        break;
    }

    return internal_opts;
}

model::offset seqno_cast(internal::sequence_number seqno) {
    return model::offset(static_cast<int64_t>(seqno()));
}

internal::sequence_number seqno_cast(model::offset o) {
    if (o < model::offset{0}) {
        throw std::invalid_argument(
          fmt::format(
            "unable to translate negative offset {} into a sequence number",
            o));
    }
    return internal::sequence_number(static_cast<uint64_t>(o()));
}

} // namespace

iterator::iterator(std::unique_ptr<internal::iterator> impl)
  : _impl(std::move(impl)) {}
iterator::iterator(iterator&&) noexcept = default;
iterator& iterator::operator=(iterator&&) noexcept = default;
iterator::~iterator() noexcept = default;

bool iterator::valid() const { return _impl->valid(); }

ss::future<> iterator::seek_to_first() { return _impl->seek_to_first(); }
ss::future<> iterator::seek_to_last() { return _impl->seek_to_last(); }
ss::future<> iterator::seek(std::string_view target) {
    auto key = internal::key::encode({
      .key = target,
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    co_await _impl->seek(key);
}
ss::future<> iterator::next() { return _impl->next(); }
ss::future<> iterator::prev() { return _impl->prev(); }
std::string_view iterator::key() { return _impl->key().user_key(); }
iobuf iterator::value() { return _impl->value(); }

database::database(std::unique_ptr<db::impl> impl)
  : _impl(std::move(impl)) {}
database::database(database&&) noexcept = default;
database& database::operator=(database&&) noexcept = default;
database::~database() noexcept = default;

ss::future<database> database::open(options opts, io::persistence p) {
    auto impl = co_await db::impl::open(translate_options(opts), std::move(p));
    co_return database(std::move(impl));
}

ss::future<> database::close() { return _impl->close(); }

model::offset database::max_persisted_offset() const {
    return seqno_cast(_impl->max_persisted_seqno());
}

model::offset database::max_applied_offset() const {
    return seqno_cast(_impl->max_applied_seqno());
}

ss::future<> database::flush() { return _impl->flush(); }

ss::future<> database::apply(write_batch batch) {
    auto b = std::move(batch._batch);
    co_await _impl->apply(std::move(b));
}

ss::future<std::optional<iobuf>> database::get(std::string_view target) {
    auto key = internal::key::encode({
      .key = target,
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    auto result = co_await _impl->get(key);
    co_return std::move(result).take_value();
}

ss::future<iterator> database::create_iterator() {
    auto iter = co_await _impl->create_iterator(std::nullopt);
    co_return iterator(std::move(iter));
}

write_batch database::create_write_batch() { return write_batch{_impl.get()}; }

write_batch::write_batch(db::impl* db)
  : _batch(ss::make_lw_shared<db::memtable>())
  , _db(db) {}
write_batch::write_batch(write_batch&&) noexcept = default;
write_batch& write_batch::operator=(write_batch&&) noexcept = default;
write_batch::~write_batch() noexcept = default;

void write_batch::put(std::string_view key, iobuf value, model::offset offset) {
    auto k = internal::key::encode({
      .key = key,
      .seqno = seqno_cast(offset),
      .type = internal::value_type::value,
    });
    _batch->put(std::move(k), std::move(value));
}

void write_batch::remove(std::string_view key, model::offset offset) {
    auto k = internal::key::encode({
      .key = key,
      .seqno = seqno_cast(offset),
      .type = internal::value_type::tombstone,
    });
    _batch->remove(std::move(k));
}

ss::future<std::optional<iobuf>> write_batch::get(std::string_view target) {
    auto key = internal::key::encode({
      .key = target,
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    auto result = _batch->get(key);
    if (result.is_missing()) {
        result = co_await _db->get(key);
    }
    co_return std::move(result).take_value();
}

ss::future<iterator> write_batch::create_iterator() {
    auto iter = co_await _db->create_iterator(_batch);
    co_return iterator(std::move(iter));
}

} // namespace lsm
