/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/db/flush_actor.h"

#include "base/vlog.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/logger.h"
#include "lsm/db/table_builder.h"

#include <seastar/util/defer.hh>

namespace lsm::db {

using internal::operator""_level;

ss::future<> flush_actor::process(flush_message msg) {
    auto _ = ss::defer([this] { _active = false; });
    auto v = _versions->current();
    auto id = _versions->new_file_id();
    auto& imm = msg.immutable_memtable;
    auto level = imm->empty()
                   ? 0_level
                   : v->pick_level_for_memtable_output(
                       imm->min_key().user_key(), imm->max_key().user_key());
    auto mem_bytes = imm->approximate_memory_usage();
    vlog(
      log.trace,
      "flush_memtable_start level={} file_id={} mem_bytes={}",
      level,
      id,
      mem_bytes);
    sst::builder::options sst_options{
      .block_size = _opts->sst_block_size,
      .filter_period = _opts->sst_filter_period,
      .compression = _opts->levels[level].compression,
    };
    auto result = co_await build_table(
      _persistence,
      {.id = id, .epoch = _opts->database_epoch},
      imm->create_iterator(),
      sst_options,
      &_as);
    if (!result) {
        _versions->reuse_file_id(id);
        vlog(
          log.trace,
          "flush_memtable_end level={} file_bytes=0 empty=true",
          level);
        co_return;
    }
    version_edit edit(*_opts);
    edit.set_last_seqno(result->newest_seqno);
    edit.add_file({
      .level = level,
      .file_handle = {.id = id, .epoch = _opts->database_epoch},
      .file_size = result->file_size,
      .smallest = std::move(result->smallest),
      .largest = std::move(result->largest),
      .oldest_seqno = result->oldest_seqno,
      .newest_seqno = result->newest_seqno,
    });
    co_await _manifest_actor->tell(
      manifest_update_message{
        .edit = std::move(edit),
      });
    vlog(
      log.trace,
      "flush_memtable_end level={} file_id={} file_bytes={}",
      level,
      id,
      result->file_size);
}

void flush_actor::on_error(std::exception_ptr ex) noexcept {
    vlog(log.warn, "flush_memtable_end error={}", ex);
}
} // namespace lsm::db
