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
#include "bytes/iobuf.h"
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "pandaproxy/schema_registry/types.h"
#include "ssx/sformat.h"
#include "test_utils/test.h"

#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <cstddef>
#include <string_view>
#include <vector>

namespace pandaproxy::schema_registry::rest_client {

namespace {

// Build an iobuf whose bytes are split into separate fragments of at most
// `chunk_size` bytes, to exercise the streaming parser across fragment
// boundaries (including values that span fragments).
iobuf fragmented_iobuf(std::string_view s, size_t chunk_size) {
    iobuf buf;
    for (size_t i = 0; i < s.size(); i += chunk_size) {
        auto piece = s.substr(i, chunk_size);
        ss::temporary_buffer<char> frag{piece.size()};
        std::ranges::copy(piece, frag.get_write());
        buf.append(std::move(frag));
    }
    return buf;
}

} // namespace

TEST_CORO(parse_subjects_test, empty_array) {
    auto res = co_await parse_subjects(
      iobuf::from("[]"), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_TRUE_CORO(res.value().empty());
}

TEST_CORO(parse_subjects_test, mixed_subjects_enabled) {
    // bare, qualified, colon-in-subject, leading-':'-without-'.', and explicit
    // default context. Element order is preserved.
    auto res = co_await parse_subjects(
      iobuf::from(
        R"(["bare", ":.ctx:sub", ":.ctx:a:b:c", ":no-dot", ":.:def"])"),
      qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{5});
    ASSERT_EQ_CORO(s[0], context_subject(default_context, subject{"bare"}));
    ASSERT_EQ_CORO(s[1], context_subject(context{".ctx"}, subject{"sub"}));
    ASSERT_EQ_CORO(s[2], context_subject(context{".ctx"}, subject{"a:b:c"}));
    ASSERT_EQ_CORO(s[3], context_subject(default_context, subject{":no-dot"}));
    ASSERT_EQ_CORO(s[4], context_subject(default_context, subject{"def"}));
}

TEST_CORO(parse_subjects_test, qualified_disabled_is_literal) {
    // With the policy disabled, a ":.ctx:sub" element is taken verbatim as a
    // default-context subject rather than being split.
    auto res = co_await parse_subjects(
      iobuf::from(R"([":.ctx:sub", "bare"])"), qualified_subjects_enabled::no);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{2});
    ASSERT_EQ_CORO(
      s[0], context_subject(default_context, subject{":.ctx:sub"}));
    ASSERT_EQ_CORO(s[1], context_subject(default_context, subject{"bare"}));
}

TEST_CORO(parse_subjects_test, trailing_content_after_array_is_error) {
    // The subjects body is exactly one array; content after the closing ']' is
    // rejected, not ignored.
    for (std::string_view body :
         {R"(["a"] "more")", R"(["a"][])", R"(["a"]garbage)", R"(["a"],)"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, trailing_whitespace_is_ok) {
    // Whitespace after the array is fine; the parser skips it to reach EOF.
    auto res = co_await parse_subjects(
      iobuf::from("[\"a\"]  \n\t "), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    ASSERT_EQ_CORO(res.value().size(), size_t{1});
    ASSERT_EQ_CORO(
      res.value()[0], context_subject(default_context, subject{"a"}));
}

TEST_CORO(parse_subjects_test, not_an_array_is_error) {
    for (std::string_view body :
         {R"({})", R"("just-a-string")", "42", "null", "true"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, non_string_element_is_error) {
    for (std::string_view body :
         {R"(["ok", 42])",
          R"(["ok", null])",
          R"(["ok", {}])",
          R"(["ok", ["nested"]])"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, malformed_or_truncated_is_error) {
    for (std::string_view body :
         {"", "[", R"(["a")", R"(["a",)", R"(["unterminated)", "not json"}) {
        SCOPED_TRACE(body);
        auto res = co_await parse_subjects(
          iobuf::from(body), qualified_subjects_enabled::yes);
        ASSERT_FALSE_CORO(res.has_value());
    }
}

TEST_CORO(parse_subjects_test, fragmented_input) {
    // One byte per fragment: forces subjects (and the array structure) to span
    // fragment boundaries.
    constexpr std::string_view body = R"(["bare", ":.ctx:sub", ":.ctx:a:b:c"])";
    auto res = co_await parse_subjects(
      fragmented_iobuf(body, 1), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), size_t{3});
    ASSERT_EQ_CORO(s[0], context_subject(default_context, subject{"bare"}));
    ASSERT_EQ_CORO(s[1], context_subject(context{".ctx"}, subject{"sub"}));
    ASSERT_EQ_CORO(s[2], context_subject(context{".ctx"}, subject{"a:b:c"}));
}

TEST_CORO(parse_subjects_test, round_trip) {
    // Build the wire form from a set of subjects, parse it back, and assert we
    // recover the originals (a lightweight stand-in until fuzzing exists).
    std::vector<context_subject> expected{
      context_subject(default_context, subject{"bare"}),
      context_subject(context{".ctx"}, subject{"sub"}),
      context_subject(context{".env"}, subject{"topic-value"}),
      context_subject(default_context, subject{"with.dots"}),
    };

    iobuf body;
    body.append("[", 1);
    for (size_t i = 0; i < expected.size(); ++i) {
        auto elem = ssx::sformat(
          R"({}"{}")", i == 0 ? "" : ",", expected[i].to_string());
        body.append(elem.data(), elem.size());
    }
    body.append("]", 1);

    auto res = co_await parse_subjects(
      std::move(body), qualified_subjects_enabled::yes);
    ASSERT_TRUE_CORO(res.has_value());
    const auto& s = res.value();
    ASSERT_EQ_CORO(s.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_EQ_CORO(s[i], expected[i]);
    }
}

} // namespace pandaproxy::schema_registry::rest_client
