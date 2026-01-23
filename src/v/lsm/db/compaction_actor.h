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

#pragma once

#include "lsm/core/internal/options.h"
#include "lsm/db/manifest_actor.h"
#include "lsm/db/snapshot.h"
#include "lsm/db/version_set.h"
#include "lsm/io/persistence.h"
#include "ssx/actor.h"

namespace lsm::db {

// An actor to perform compaction tasks.
//
// Currently there is only a single compaction being performed at a time.
class compaction_actor : ssx::actor<compaction, 1> {
    using super = ssx::actor<compaction, 1>;

public:
    compaction_actor(
      io::data_persistence* persistence,
      snapshot_list* snapshots,
      manifest_actor* manifest_actor,
      version_set* versions,
      ss::lw_shared_ptr<internal::options> opts)
      : _persistence(persistence)
      , _snapshots(snapshots)
      , _manifest_actor(manifest_actor)
      , _versions(versions)
      , _opts(std::move(opts)) {}

    using super::start;
    using super::stop;

    bool is_idle() const { return !_active; }

    void tell(compaction c) {
        vassert(is_idle(), "cannot trigger double compaction work");
        _active = true;
        std::ignore = super::try_tell(std::move(c));
    }

protected:
    ss::future<> process(compaction compaction) override;

    void on_error(std::exception_ptr ex) noexcept override;

private:
    io::data_persistence* _persistence;
    snapshot_list* _snapshots;
    manifest_actor* _manifest_actor;
    version_set* _versions;
    ss::lw_shared_ptr<internal::options> _opts;
    bool _active = false;
};

} // namespace lsm::db
