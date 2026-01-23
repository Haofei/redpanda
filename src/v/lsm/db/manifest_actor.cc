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

#include "lsm/db/manifest_actor.h"

#include "base/vlog.h"
#include "lsm/core/internal/logger.h"

namespace lsm::db {

ss::future<> manifest_actor::process(manifest_update_message msg) {
    vlog(log.trace, "manifest_actor_process_start");
    co_await _versions->log_and_apply(std::move(msg.edit));
    // TODO: only call GC actor when we delete a file. This requires
    // the GC actor still doing cleanup when it is not called.
    _gc_actor->tell(
      gc_message{
        .highest_used_file_id = _versions->highest_used_file_id(),
        .live_files = _versions->get_live_files(),
      });
    _write_callback();
    vlog(log.trace, "manifest_actor_process_end");
}

void manifest_actor::on_error(std::exception_ptr ex) noexcept {
    vlog(log.warn, "manifest_actor_process_end error=\"{}\"", ex);
}

} // namespace lsm::db
