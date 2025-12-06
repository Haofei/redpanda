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

#include "base/seastarx.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <memory>

namespace lsm {

namespace db {
class impl;
class memtable;
} // namespace db

namespace internal {
class iterator;
} // namespace internal

class iterator;

// Options for the database.
struct options {};

class write_batch;

// A LSM tree database. Note that this database does *not* have a WAL.
class database {
public:
    explicit database(std::unique_ptr<db::impl> impl);
    database(const database&) = delete;
    database(database&&) noexcept;
    database& operator=(const database&) = delete;
    database& operator=(database&&) noexcept;
    ~database() noexcept;

    // Open the database.
    static ss::future<database> open(options, io::persistence);

    // Close the database, no more operations should happen to the database at
    // this point, and all iterators should be closed before calling this
    // method.
    //
    // This *must* be called before destroying the database.
    ss::future<> close();

    // The maximum offset that has been persisted to durable storage.
    model::offset max_persisted_offset() const;

    // The maximum offset that has been applied to the database (persisted or
    // not).
    model::offset max_applied_offset() const;

    // Flush existing buffered data such that that `max_persisted_offset()`
    // becomes >= the current `max_applied_offset()`.
    ss::future<> flush();

    // Apply a batch of data atomically to the database.
    //
    // Any gets/iteration on the write batch must be finished before calling
    // this method.
    ss::future<> apply(write_batch);

    // Lookup a value in the database
    ss::future<std::optional<iobuf>> get(std::string_view key);

    // Create an iterator over the database.
    //
    // This iterator gives snapshot isolation, so writes applied after
    // this future completes will not be seen by the iterator.
    ss::future<iterator> create_iterator();

    // Create a write batch that can be applied to the the database.
    //
    // This write batch can only be used with this database and it's lifetime is
    // tied to this database as well.
    write_batch create_write_batch();

private:
    std::unique_ptr<db::impl> _impl;
};

// An iterator over the contents of the database.
class iterator {
public:
    explicit iterator(std::unique_ptr<internal::iterator> impl);
    iterator(const iterator&) = delete;
    iterator(iterator&&) noexcept;
    iterator& operator=(const iterator&) = delete;
    iterator& operator=(iterator&&) noexcept;
    ~iterator() noexcept;

    // An iterator is either positioned at a key/value pair, or
    // not valid. This method returns true iff the iterator is valid.
    bool valid() const;

    // Position at the first key in the source. The iterator is valid()
    // after this call iff the source is not empty.
    ss::future<> seek_to_first();

    // Position at the last key in the source. The iterator is
    // valid() after this call iff the source is not empty.
    ss::future<> seek_to_last();

    // Position at the first key in the source that is at or past target.
    // The iterator is valid() after this call iff the source contains
    // an entry that comes at or past target.
    ss::future<> seek(std::string_view target);

    // Moves to the next entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: valid()
    ss::future<> next();

    // Moves to the previous entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: valid()
    ss::future<> prev();

    // Return the key for the current entry. The returned value is only valid
    // until the iterator is moved.
    // REQUIRES: valid()
    std::string_view key();

    // Return the value for the current entry.
    // REQUIRES: valid()
    iobuf value();

private:
    std::unique_ptr<internal::iterator> _impl;
};

// A batch of data that can be applied atomically to the database.
//
// In addition to being a staging ground for write operations,
// the write batch also supports reading the staged data as a view over the
// existing database so that you can Read-Your-Own-Writes before data is
// committed to the LSM tree.
class write_batch {
public:
    write_batch(const write_batch&) = delete;
    write_batch(write_batch&&) noexcept;
    write_batch& operator=(const write_batch&) = delete;
    write_batch& operator=(write_batch&&) noexcept;
    ~write_batch() noexcept;

    // Set the key in the database with the given value for this offset.
    //
    // REQUIRES: offsets must be monotonically increasing as added to the batch.
    void put(std::string_view key, iobuf value, model::offset offset);

    // Remove the key in the database at this offset.
    //
    // REQUIRES: offsets must be monotonically increasing as added to the batch.
    void remove(std::string_view key, model::offset);

    // Lookup a value in the database as if the current write batch was applied.
    //
    // The returned future must finish resolving before the write_batch is
    // applied to the database.
    ss::future<std::optional<iobuf>> get(std::string_view key);

    // Create an iterator over the database as if the current write batch was
    // applied.
    //
    // This iterator gives snapshot isolation, so  writes applied after
    // this future completes will not be seen by the iterator as long as
    // these writes do not have the same offset.
    //
    // The resulting iterator is safe to use concurrently with additional writes
    // being applied to the batch.
    //
    // The returned iterator must not be used after the write_batch is applied
    // to the database.
    ss::future<iterator> create_iterator();

private:
    explicit write_batch(db::impl*);

    friend class database;
    ss::lw_shared_ptr<db::memtable> _batch;
    db::impl* _db;
};

} // namespace lsm
