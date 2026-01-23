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

#include "lsm/db/gc_actor.h"
#include "lsm/db/version_edit.h"
#include "lsm/db/version_set.h"
#include "ssx/actor.h"

namespace lsm::db {

struct manifest_update_message {
    version_edit edit;
};

// An actor to linearize and apply updates to the LSM tree.
//
// We use a mailbox size of two so that the flush and compaction actors can run
// independantly. Those two processes are safe to run concurrently because:
// 1. they all use a shared version set and coordinate file IDs
// 2. version_edits cannot overlap between them (flush is insert only)
// 3. currently there is only a single flush and compaction actor
//
// TODO: It would be nice to be able to slurp up all the messages in batches
// here and combine and apply them together to reduce the amorize the cost in
// writing a manifest. We'll have to figure out if that is an actor or something
// else.
class manifest_actor : public ssx::actor<manifest_update_message, 2> {
public:
    manifest_actor(
      version_set* versions,
      gc_actor* gc_actor,
      ss::noncopyable_function<void()> write_callback)
      : _versions(versions)
      , _gc_actor(gc_actor)
      , _write_callback(std::move(write_callback)) {}

protected:
    ss::future<> process(manifest_update_message msg) override;

    void on_error(std::exception_ptr ex) noexcept override;

private:
    version_set* _versions;
    gc_actor* _gc_actor;
    ss::noncopyable_function<void()> _write_callback;
};

} // namespace lsm::db
