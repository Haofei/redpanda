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

#pragma once

#include "lsm/core/internal/options.h"
#include "lsm/db/manifest_actor.h"
#include "lsm/db/memtable.h"
#include "lsm/db/version_set.h"
#include "lsm/io/persistence.h"
#include "ssx/actor.h"

namespace lsm::db {

// A request to the GC actor to remove old unused files.
struct flush_message {
    ss::lw_shared_ptr<memtable> immutable_memtable;
};

// An actor to flush memtables to L0 files in the LSM tree.
class flush_actor : ssx::actor<flush_message, 1> {
    using super = ssx::actor<flush_message, 1>;

public:
    flush_actor(
      ss::lw_shared_ptr<internal::options> opts,
      io::data_persistence* persistence,
      version_set* versions,
      manifest_actor* manifest_actor)
      : _opts(std::move(opts))
      , _persistence(persistence)
      , _versions(versions)
      , _manifest_actor(manifest_actor) {}

    using super::start;
    using super::stop;

    bool is_idle() const { return !_active; }

    void tell(flush_message msg) {
        vassert(is_idle(), "cannot trigger double flush work");
        _active = true;
        std::ignore = super::try_tell(std::move(msg));
    }

protected:
    ss::future<> process(flush_message msg) override;

    void on_error(std::exception_ptr ex) noexcept override;

private:
    ss::lw_shared_ptr<internal::options> _opts;
    io::data_persistence* _persistence;
    version_set* _versions;
    manifest_actor* _manifest_actor;
    bool _active = false;
};

} // namespace lsm::db
