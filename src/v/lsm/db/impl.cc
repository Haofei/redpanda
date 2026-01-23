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

#include "lsm/db/impl.h"

#include "base/vlog.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/logger.h"
#include "lsm/core/internal/merging_iterator.h"
#include "lsm/db/iter.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/block_cache.h"
#include "ssx/clock.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <memory>
#include <utility>

namespace lsm::db {

using internal::operator""_level;

impl::impl(ctor, io::persistence p, ss::lw_shared_ptr<internal::options> o)
  : _persistence(std::move(p))
  , _opts(std::move(o))
  , _mem(ss::make_lw_shared<memtable>())
  , _table_cache(
      std::make_unique<table_cache>(
        _persistence.data.get(),
        _opts->max_open_files,
        ss::make_lw_shared<sst::block_cache>(
          _opts->block_cache_size / _opts->sst_block_size)))
  , _versions(
      std::make_unique<version_set>(
        _persistence.metadata.get(), _table_cache.get(), _opts))
  , _gc_actor(_persistence.data.get(), _opts, _table_cache.get())
  , _manifest_actor(_versions.get(), &_gc_actor, [this] { on_new_manifest(); })
  , _flush_actor(
      _opts, _persistence.data.get(), _versions.get(), &_manifest_actor)
  , _compaction_actor(
      _persistence.data.get(),
      &_snapshots,
      &_manifest_actor,
      _versions.get(),
      _opts) {}

ss::future<std::unique_ptr<impl>> impl::open(
  ss::lw_shared_ptr<internal::options> opts, io::persistence persistence) {
    vlog(log.trace, "open_start");
    auto db = std::make_unique<impl>(
      ctor{}, std::move(persistence), std::move(opts));
    auto fut = co_await ss::coroutine::as_future(db->recover());
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_await db->close().handle_exception([](const std::exception_ptr&) {});
        std::rethrow_exception(ex);
    }
    // If we're readonly, we don't need to start any compaction loop.
    if (db->_opts->readonly) {
        vlog(log.trace, "open_end readonly=true");
        co_return db;
    }
    co_await db->_gc_actor.start();
    co_await db->_flush_actor.start();
    co_await db->_compaction_actor.start();
    co_await db->_manifest_actor.start();
    vlog(log.trace, "open_end readonly=false");
    co_return db;
}

ss::future<> impl::apply(ss::lw_shared_ptr<memtable> batch) {
    if (_opts->readonly) [[unlikely]] {
        throw invalid_argument_exception(
          "attempted to write to a readonly database");
    }
    if (batch->empty()) {
        co_return;
    }
    co_await make_room_for_write();
    _mem->merge(std::move(batch));
}

ss::future<> impl::make_room_for_write() {
    bool allow_delay = true;
    while (true) {
        if (
          allow_delay
          && _versions->current()->num_files(0_level)
               > _opts->level_zero_slowdown_writes_trigger) {
            // We're in throttling mode
            vlog(log.debug, "throttling_writes reason=l0_file_count");
            try {
                co_await ss::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                throw abort_requested_exception(
                  "shutdown requested during write throttling");
            }
            // Only throttle once.
            allow_delay = false;
            continue;
        }
        if (_mem->approximate_memory_usage() <= _opts->write_buffer_size) {
            // We're under our write buffer limit, let's proceed
            // Note there is a scheduling point here, so this is a soft
            // limit.
            co_return;
        }
        if (_imm) {
            vlog(log.warn, "blocking_writes reason=memtable_full");
            // We are over the write buffer limit and we have a pending
            // memtable flush, wait for it to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        if (
          _versions->current()->num_files(0_level)
          > _opts->level_zero_stop_writes_trigger) {
            vlog(log.warn, "blocking_writes reason=l0_full");
            // We've hit out L0 file limit, wait for compaction to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        // We're over our limit, let's make a new memtable
        vlog(
          log.trace,
          "scheduling_memtable_flush seqno={}",
          _mem->last_seqno().value());
        _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
        maybe_schedule_compaction();
    }
}

ss::future<lookup_result> impl::get(internal::key_view key) {
    // Lookup in the mutable memtable
    {
        auto result = _mem->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the frozen memtable
    if (_imm) {
        auto result = (*_imm)->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the files
    auto current = _versions->current();
    version::get_stats stats{};
    auto result = co_await current->get(key, &stats);
    if (current->update_stats(stats)) {
        maybe_schedule_compaction();
    }
    co_return result;
}

ss::future<std::unique_ptr<internal::iterator>>
impl::create_iterator(iterator_options opts) {
    std::optional<internal::sequence_number> iter_seqno = max_applied_seqno();
    std::unique_ptr<internal::iterator> iter;
    if (!iter_seqno) {
        // If there is no data in the database, then we create
        // a view of empty data, since we cannot pin before 0.
        iter = internal::iterator::create_empty();
    } else {
        iter = create_db_iterator(
          co_await create_internal_iterator(),
          opts.snapshot ? (*opts.snapshot)->seqno() : iter_seqno.value(),
          _opts,
          [this](internal::key_view key) {
              return _versions->current()->record_read_sample(key).then(
                [this](bool compaction_needed) {
                    if (compaction_needed) {
                        maybe_schedule_compaction();
                    }
                });
          });
    }
    // If there is a non-empty memtable, wrap our existing iterator on top of
    // it and clamp further writes to the memtable to be applied.
    if (auto table = opts.memtable->get(); table && !table->empty()) {
        chunked_vector<std::unique_ptr<internal::iterator>> merged;
        merged.push_back(table->create_iterator());
        merged.push_back(std::move(iter));
        iter = create_db_iterator(
          internal::create_merging_iterator(std::move(merged)),
          table->last_seqno().value(),
          _opts,
          [](internal::key_view) { return ss::now(); });
    }
    co_return iter;
}

ss::future<std::unique_ptr<internal::iterator>>
impl::create_internal_iterator() {
    chunked_vector<std::unique_ptr<internal::iterator>> list;
    list.push_back(_mem->create_iterator());
    if (_imm) {
        list.push_back((*_imm)->create_iterator());
    }
    co_await _versions->current()->add_iterators(&list);
    co_return internal::create_merging_iterator(std::move(list));
}

ss::future<> impl::flush(ssx::instant deadline) {
    if (_opts->readonly) [[unlikely]] {
        throw invalid_argument_exception(
          "attempted to flush a readonly database");
    }
    auto applied_seqno = max_applied_seqno();
    while (applied_seqno > max_persisted_seqno()) {
        if (ssx::lowres_steady_clock().now() > deadline) {
            throw io_error_exception(
              "failed to persist up to seqno {} in time: current persisted "
              "seqno {}",
              applied_seqno.value_or(internal::sequence_number(0)),
              max_persisted_seqno().value_or(internal::sequence_number(0)));
        }
        if (_imm) {
            co_await _background_work_finished_signal.wait(
              deadline.to_chrono<ss::lowres_clock>(), _as);
        } else if (!_mem->empty()) {
            _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
            maybe_schedule_compaction();
        }
    }
}

ss::future<> impl::flush() {
    return impl::flush(ssx::instant::infinite_future());
}

ss::future<> impl::close() {
    vlog(log.trace, "close_start");
    _as.request_abort_ex(abort_requested_exception("database closing"));
    co_await _gc_actor.stop();
    co_await _compaction_actor.stop();
    co_await _flush_actor.stop();
    co_await _manifest_actor.stop();
    co_await _table_cache->close();
    co_await _persistence.data->close();
    co_await _persistence.metadata->close();
    vlog(log.trace, "close_end");
}

ss::future<> impl::recover() {
    vlog(log.trace, "recover_start");
    co_await _versions->recover();
    // If requested, then pre-open all the files we know about.
    if (auto max_fibers = _opts->max_pre_open_fibers) {
        chunked_vector<ss::lw_shared_ptr<file_meta_data>> all_files;
        for (auto level : _opts->levels) {
            auto files = _versions->current()->get_overlapping_inputs(
              level.number, std::nullopt, std::nullopt);
            for (auto& file : files) {
                all_files.push_back(std::move(file));
            }
        }
        vlog(log.trace, "recover_pre_open_start files={}", all_files.size());
        co_await ss::max_concurrent_for_each(
          all_files, max_fibers, [this](ss::lw_shared_ptr<file_meta_data> f) {
              return _table_cache->create_iterator(f->handle, f->file_size)
                .discard_result();
          });
        vlog(log.trace, "recover_pre_open_end files={}", all_files.size());
    }
    vlog(log.trace, "recover_end");
}

void impl::maybe_schedule_compaction() {
    if (_flush_actor.is_idle() && _imm) {
        _flush_actor.tell(flush_message{.immutable_memtable = *_imm});
    }
    if (_compaction_actor.is_idle() && _versions->needs_compaction()) {
        _compaction_actor.tell(_versions->pick_compaction().value());
    }
}

void impl::on_new_manifest() {
    vlog(
      log.trace,
      "new_manifest_applied seqno={}",
      max_persisted_seqno().value());
    // Wait for the memtable to be applied
    if (_imm && max_persisted_seqno() >= (*_imm)->last_seqno()) {
        // Now that the new version has been applied, it's safe to remove the
        // immutable memtable, as readers will pick up the new file instead.
        //
        // Note that it's possible for a reader to pick up both the memtable
        // and the new version with the file. This is OK because all iterators
        // deduplicate already.
        _imm = std::nullopt;
    }
    // Notify all waiters that work has been finished.
    _background_work_finished_signal.broadcast();
    // Check and see if the new manifest we wrote requires compaction due to
    // level size limits.
    maybe_schedule_compaction();
}

std::optional<internal::sequence_number> impl::max_persisted_seqno() const {
    return _versions->last_seqno();
}

std::optional<internal::sequence_number> impl::max_applied_seqno() const {
    if (auto seqno = _mem->last_seqno()) {
        return seqno;
    }
    if (_imm) {
        if (auto seqno = (*_imm)->last_seqno()) {
            return seqno;
        }
    }
    return max_persisted_seqno();
}

ss::optimized_optional<std::unique_ptr<snapshot>> impl::create_snapshot() {
    if (auto seqno = max_applied_seqno()) {
        return _snapshots.create(*seqno);
    }
    return std::nullopt;
}

} // namespace lsm::db
