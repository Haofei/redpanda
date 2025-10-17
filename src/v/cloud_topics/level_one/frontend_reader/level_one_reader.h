/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/log_reader_config.h"
#include "model/record_batch_reader.h"
#include "utils/prefix_logger.h"

namespace cloud_topics {

/*
 * This class implements a record batch reader for level one.
 *
 * The reader is a state machine with the following states:
 * - empty: no metadata is cached, no data is materialized
 * - ready: metadata is available but no data is materialized
 * - materialized: the reader contains materialized batches
 * - end_of_stream: no more data to read
 *
 * The state transitions are:
 *              ┌───┐
 *              │EOS├───┤Terminate│
 *              └───┘
 *                ▲
 *                │
 *              ┌─┴───┐   ┌─────┐
 * │Init├──────►│empty├──►│ready│
 *              └─────┘   └──┬──┘
 *                  ▲        │
 *                  │        ▼
 *                ┌─┴──────────┐
 *                │materialized│
 *                └────────────┘
 *
 * The reader starts in the 'empty' state.
 *
 * In 'empty' state, the reader queries the metastore to find the L1 object
 * containing data at or after the requested offset. When metadata is found,
 * it transitions to 'ready' state. If no data is available, it transitions
 * to 'end_of_stream'.
 *
 * In 'ready' state, the reader reads the object footer to determine what
 * partition data to materialize, then fetches the required ranges from
 * object storage. When batches are materialized, it transitions to
 * 'materialized' state.
 *
 * In 'materialized' state, data can be consumed. When all materialized
 * batches are consumed, the reader transitions back to 'empty' state
 * and queries the metastore for the next L1 object partition that.
 */
class level_one_log_reader_impl : public model::record_batch_reader::impl {
public:
    level_one_log_reader_impl(
      const cloud_topic_log_reader_config& cfg,
      model::ntp ntp,
      model::topic_id_partition tidp,
      l1::metastore* metastore,
      l1::io* io_interface);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    void print(std::ostream& o) final;

private:
    enum class state {
        empty,
        ready,
        materialized,
        end_of_stream,
    };

    // Represents the current L1 object being read.
    struct current_object {
        l1::object_id oid;
        l1::footer footer;
        kafka::offset last_offset;
    };

    /*
     * Contacts the L1 metastore to retrieve metadata for an L1 object that
     * contains the target offset.
     */
    ss::future<std::optional<current_object>> lookup_object_for_offset(
      kafka::offset, model::timeout_clock::time_point deadline);

    /*
     * Materialize batches from the L1 object starting from the given offset.
     */
    ss::future<chunked_circular_buffer<model::record_batch>>
    materialize_batches_from_object_offset(
      const current_object&,
      kafka::offset,
      model::timeout_clock::time_point deadline);

    /*
     * Return batches from the reader's current position until the next
     * partition or the end of the object is reached. The set of batches
     * returned may further be limited by restrictions (e.g. byte limit)
     * imposed by the reader configuration.
     */
    ss::future<chunked_circular_buffer<model::record_batch>>
    read_batches(l1::object_reader& reader);

    /*
     * Prepare the result set to return to the record batch reader and configure
     * the reader for the next request to load slice which will be the next
     * offset that should be fetched.
     */
    chunked_circular_buffer<model::record_batch> consume_materialized_batches();

    ss::future<l1::footer>
    read_footer(l1::object_id oid, size_t footer_pos, size_t object_size);

    bool is_over_limit(size_t size) const;

    ss::future<> close_reader_safe(std::unique_ptr<l1::object_reader>&);

    state _state{state::empty};

    // Current object being processed.
    // Present in ready and materialized states, empty in empty state.
    std::optional<current_object> _current_obj;

    // Materialized batches ready to be consumed.
    chunked_circular_buffer<model::record_batch> _batches;

    cloud_topic_log_reader_config _config;
    model::ntp _ntp;
    model::topic_id_partition _tidp;
    kafka::offset _next_offset;
    l1::metastore* _metastore;
    l1::io* _io;
    prefix_logger _log;
};

} // namespace cloud_topics
