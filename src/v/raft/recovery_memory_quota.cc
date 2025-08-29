/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "raft/recovery_memory_quota.h"

#include "base/vlog.h"
#include "raft/logger.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/semaphore.h"

#include <seastar/core/memory.hh>

namespace raft {

recovery_memory_quota::recovery_memory_quota(
  config::binding<std::optional<size_t>> max_recovery_memory,
  config::binding<size_t> default_read_buffer_size,
  config::binding<size_t> raft_recovery_concurrency_per_shard)
  : _max_recovery_memory(std::move(max_recovery_memory))
  , _default_read_buffer_size(std::move(default_read_buffer_size))
  , _raft_recovery_concurrency_per_shard(
      std::move(raft_recovery_concurrency_per_shard))
  , _current_max_recovery_mem(
      _max_recovery_memory().value_or(memory_groups().recovery_max_memory()))
  , _memory(_current_max_recovery_mem, "raft/recovery-quota") {
    _max_recovery_memory.watch([this] { on_max_memory_changed(); });
}

ss::future<ssx::semaphore_units> recovery_memory_quota::acquire_read_memory() {
    auto size = std::min(
      _default_read_buffer_size(),
      _max_recovery_memory().value_or(memory_groups().recovery_max_memory())
        / _raft_recovery_concurrency_per_shard());
    return ss::get_units(_memory, size);
}

void recovery_memory_quota::on_max_memory_changed() {
    int64_t new_size = int64_t(
      _max_recovery_memory().value_or(memory_groups().recovery_max_memory()));

    vlog(raftlog.info, "max recovery memory changed to {} bytes", new_size);

    auto diff = new_size - static_cast<int64_t>(_current_max_recovery_mem);
    if (diff < 0) {
        _memory.consume(-diff);
    } else if (diff > 0) {
        _memory.signal(diff);
    }
    _current_max_recovery_mem = new_size;
}

} // namespace raft
