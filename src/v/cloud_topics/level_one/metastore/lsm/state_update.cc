/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state_update.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state_update.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

namespace {

// Checks the input new objects and ensures that they don't already exist in
// the state. Collects the input extents and objects into the input maps.
//
// NOTE: the returned total_data_size fields are not populated and must be
// populated after determining whether to accept the extents.
ss::future<std::expected<void, db_update_error>> validate_new_objects_missing(
  const chunked_vector<new_object>& new_objects,
  state_reader& state,
  sorted_extents_by_tidp_t& out_extents,
  chunked_hash_map<object_id, object_entry>& out_objects) {
    for (const auto& o : new_objects) {
        auto object_res = co_await state.get_object(o.oid);
        if (!object_res.has_value()) {
            co_return std::unexpected(
              db_update_error{fmt::format("Error getting object {}", o.oid)});
        }
        if (object_res.value().has_value()) {
            co_return std::unexpected(
              db_update_error{fmt::format("Object {} already exists", o.oid)});
        }
        o.collect_extents_by_tidp(&out_extents);
        out_objects.emplace(
          o.oid,
          object_entry{
            .total_data_size = 0,
            .removed_data_size = 0,
            .footer_pos = o.footer_pos,
            .object_size = o.object_size,
          });
    }
    co_return std::expected<void, db_update_error>{};
}

} // namespace

ss::future<std::expected<void, db_update_error>>
add_objects_db_update::build_rows(
  state_reader& state,
  chunked_vector<write_batch_row>& out,
  chunked_hash_map<model::topic_id_partition, kafka::offset>* corrections)
  const {
    auto validate_res = validate_inputs();
    if (!validate_res.has_value()) {
        co_return std::unexpected(validate_res.error());
    }
    sorted_extents_by_tidp_t new_extents_by_tp;
    chunked_hash_map<object_id, object_entry> new_objects_by_oid;
    auto new_extents_res = co_await validate_new_objects_missing(
      new_objects, state, new_extents_by_tp, new_objects_by_oid);
    if (!new_extents_res.has_value()) {
        co_return std::unexpected(new_extents_res.error());
    }

    chunked_hash_map<model::topic_id_partition, kafka::offset>
      corrected_next_offsets;
    chunked_hash_map<model::topic_id_partition, chunked_vector<extent>>
      verified_extents;
    chunked_hash_map<model::topic_id_partition, metadata_row_value>
      verified_meta_vals;
    for (const auto& [tidp, extents] : new_extents_by_tp) {
        // TODO: maybe we need some mount operation that adopts a partition log
        // and allows it to start a specific offset.
        auto meta_res = co_await state.get_metadata(tidp);
        if (!meta_res.has_value()) {
            co_return std::unexpected(
              db_update_error{
                fmt::format("Error getting metadata for {}", tidp)});
        }
        auto opt = meta_res.value();
        auto expected_next = opt ? opt->next_offset : kafka::offset{0};

        if (extents.begin()->base_offset != expected_next) {
            // If the start of the new extents for this partition aren't
            // aligned, allow the operation to succeed, but the expectation is
            // when applying, we'll "drop" these extents.
            corrected_next_offsets[tidp] = expected_next;
            continue;
        }
        // Now that we know we'll accept this partition's extents, account for
        // their size.
        for (const auto& extent : extents) {
            new_objects_by_oid[extent.oid].total_data_size += extent.len;
            verified_extents[tidp].push_back(extent);
        }
        verified_meta_vals[tidp] = metadata_row_value{
          .start_offset = opt ? opt->start_offset : kafka::offset{0},
          .next_offset = kafka::next_offset(extents.rbegin()->last_offset),
          .compaction_epoch = opt ? opt->compaction_epoch
                                  : partition_state::compaction_epoch_t{0},
        };
    }
    // Now that we've validated the offsets of our extents, validate the terms
    // for the accepted extents.
    chunked_hash_map<model::topic_id_partition, absl::btree_set<term_start>>
      verified_terms;
    for (const auto& [tp, req_entries] : new_terms) {
        if (corrected_next_offsets.contains(tp)) {
            continue;
        }
        // First do a basic check that the incoming term entries can be
        // appended to our state without violating ordering requirements.
        auto term_res = co_await state.get_max_term(tp);
        if (!term_res.has_value()) {
            co_return std::unexpected(
              db_update_error{
                fmt::format("Error getting max term for {}", tp)});
        }
        auto term_opt = term_res.value();
        if (term_opt.has_value()) {
            auto req_first_entry = req_entries.begin();
            // NOTE: it's valid for the first requested term to be equal to the
            // last term (e.g. if leadership has not changed). The same cannot
            // be said about offsets, hence the difference in comparator.
            if (req_first_entry->term_id < term_opt->term_id) {
                co_return std::unexpected(
                  db_update_error{fmt::format(
                    "New term for {} must be >= last term: {} < {}",
                    tp,
                    req_first_entry->term_id,
                    term_opt->term_id)});
            }
            if (req_first_entry->start_offset <= term_opt->start_offset) {
                co_return std::unexpected(
                  db_update_error{fmt::format(
                    "New term for {} must start after last term: {} <= {}",
                    tp,
                    req_first_entry->start_offset,
                    term_opt->start_offset)});
            }
        }
        auto new_term_it = req_entries.begin();
        if (term_opt.has_value() && new_term_it->term_id == term_opt->term_id) {
            // If the first added term matches the back of the latest
            // tracked term, it isn't a new term.
            ++new_term_it;
        }
        verified_terms[tp].insert(new_term_it, req_entries.end());
    }
    if (corrections) {
        *corrections = std::move(corrected_next_offsets);
    }
    // Generate the rows.
    for (const auto& [oid, entry] : new_objects_by_oid) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(
              object_row_value{
                .object = entry,
              }),
          });
    }
    for (const auto& [tidp, entries] : verified_terms) {
        for (const auto& entry : entries) {
            out.emplace_back(
              write_batch_row{
                .key = term_row_key::encode(tidp, entry.term_id),
                .value = serde::to_iobuf(
                  term_row_value{
                    .term_start_offset = entry.start_offset,
                  }),
              });
        }
    }
    for (const auto& [tidp, entries] : verified_extents) {
        for (const auto& entry : entries) {
            out.emplace_back(
              write_batch_row{
                .key = extent_row_key::encode(tidp, entry.base_offset),
                .value = serde::to_iobuf(
                  extent_row_value{
                    .last_offset = entry.last_offset,
                    .max_timestamp = entry.max_timestamp,
                    .filepos = entry.filepos,
                    .len = entry.len,
                    .oid = entry.oid,
                  }),
              });
        }
    }
    for (const auto& [tidp, entry] : verified_meta_vals) {
        out.emplace_back(
          write_batch_row{
            .key = metadata_row_key::encode(tidp),
            .value = serde::to_iobuf(entry),
          });
    }
    co_return std::expected<void, db_update_error>{};
}

std::expected<void, db_update_error>
add_objects_db_update::validate_inputs() const {
    if (new_objects.empty()) {
        return std::unexpected(db_update_error{"No objects requested"});
    }
    if (new_terms.empty()) {
        return std::unexpected(db_update_error{"Missing term info in request"});
    }
    sorted_extents_by_tidp_t new_extents;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents);
    }
    for (const auto& [tidp, extents] : new_extents) {
        if (!new_terms.contains(tidp)) {
            return std::unexpected(
              db_update_error{fmt::format("Missing term info for {}", tidp)});
        }
        auto expected_next = extents.begin()->base_offset;
        for (const auto& extent : extents) {
            if (extent.base_offset != expected_next) {
                return std::unexpected(db_update_error(
                  fmt::format(
                    "Input object breaks partition {} offset ordering: "
                    "expected next: {}, actual: {}",
                    tidp,
                    expected_next,
                    extent.base_offset())));
            }
            expected_next = kafka::next_offset(extent.last_offset);
        }
    }
    for (const auto& [tidp, terms] : new_terms) {
        if (terms.empty()) {
            return std::unexpected(
              db_update_error{
                fmt::format("Empty terms requested for {}", tidp)});
        }
        auto extents_it = new_extents.find(tidp);
        if (extents_it == new_extents.end() || extents_it->second.empty()) {
            return std::unexpected(
              db_update_error{fmt::format(
                "Terms provided for a partition that has no extents", tidp)});
        }
        auto new_extents_start_offset
          = extents_it->second.begin()->base_offset();
        auto new_terms_start_offset = terms.begin()->start_offset;
        if (new_extents_start_offset != new_terms_start_offset) {
            return std::unexpected(
              db_update_error{fmt::format(
                "Extent start and term start do not match for {}: {} != {}",
                tidp,
                new_extents_start_offset,
                new_terms_start_offset)});
        }
        auto new_extents_last_offset = extents_it->second.rbegin()->last_offset;
        auto new_terms_last_start_offset = terms.back().start_offset;
        if (new_extents_last_offset < new_terms_last_start_offset) {
            return std::unexpected(
              db_update_error{fmt::format(
                "Extents end below a requested new term for {}: {} < {}",
                tidp,
                new_extents_last_offset,
                new_terms_last_start_offset)});
        }
        // Now check that the the term entries themselves (both terms and
        // offsets) are in increasing order.
        auto max_term_so_far = model::term_id{-1};
        auto max_offset_so_far = kafka::offset{-1};
        for (const auto& entry : terms) {
            if (
              entry.term_id <= max_term_so_far
              || entry.start_offset <= max_offset_so_far) {
                return std::unexpected(
                  db_update_error{fmt::format(
                    "Invalid term for {}: term={}, offset={}, "
                    "max_term_so_far={}, max_offset_so_far={}",
                    tidp,
                    entry.term_id,
                    entry.start_offset,
                    max_term_so_far,
                    max_offset_so_far)});
            }
            max_term_so_far = entry.term_id;
            max_offset_so_far = entry.start_offset;
        }
    }
    return std::expected<void, db_update_error>{};
}

} // namespace cloud_topics::l1
