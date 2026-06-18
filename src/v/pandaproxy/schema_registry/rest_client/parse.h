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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <expected>

namespace pandaproxy::schema_registry::rest_client {

/// Describes why a schema registry response body could not be parsed into the
/// expected native type.
struct parse_error {
    ss::sstring reason;
};

/// Parse the body of a `GET /subjects` response into a list of subjects.
///
/// The response is a JSON array of subject-name strings (see the Schema
/// Registry REST API); each element is decoded with
/// context_subject::from_string. \p qualified selects whether a
/// ":.context:subject" element is interpreted as context-qualified; the caller
/// supplies this policy so that parsing a remote registry's response does not
/// depend on this node's cluster config.
///
/// The body must be exactly a JSON array of strings: a non-array, a non-string
/// element, or any trailing content after the array yields a parse_error rather
/// than a partial or lenient result. (Tolerance toward unmodeled fields is
/// reserved for richer Schema Registry responses that carry optional fields;
/// the subjects listing has a single fixed shape.) The function does not throw:
/// malformed input is reported via the returned std::expected.
ss::future<std::expected<chunked_vector<context_subject>, parse_error>>
parse_subjects(iobuf body, qualified_subjects_enabled qualified);

} // namespace pandaproxy::schema_registry::rest_client
