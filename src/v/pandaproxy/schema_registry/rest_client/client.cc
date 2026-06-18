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
#include "pandaproxy/schema_registry/rest_client/client.h"

#include "bytes/iobuf.h"
#include "http/request_builder.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/rest_client/parse.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <boost/beast/http/verb.hpp>

#include <utility>
#include <variant>

namespace pandaproxy::schema_registry::rest_client {

namespace {

constexpr std::string_view accept_json = "application/json";

// If the terminal error carries an http_status_error, parse its (already
// collected) response body for the Schema Registry error_code and attach it.
// Tolerant: leaves the error unchanged when no integer error_code is present.
ss::future<domain_error> attach_error_code(domain_error err) {
    auto* call = std::get_if<http_call_error>(&err);
    if (call == nullptr) {
        co_return std::move(err);
    }
    auto* status = std::get_if<http_status_error>(call);
    if (status == nullptr) {
        co_return std::move(err);
    }
    auto parsed = co_await parse_error_body(iobuf::from(status->body));
    if (parsed.has_value()) {
        status->error_code = parsed->error_code;
    }
    co_return std::move(err);
}

} // namespace

client::client(
  std::unique_ptr<http::abstract_client> http_client,
  ss::sstring endpoint,
  std::optional<basic_auth_credentials> credentials,
  qualified_subjects_enabled qualified,
  std::unique_ptr<retry_policy> retry_policy)
  : _http_client(std::move(http_client))
  , _endpoint(std::move(endpoint))
  , _credentials(std::move(credentials))
  , _qualified(qualified)
  , _retry_policy(
      retry_policy ? std::move(retry_policy)
                   : std::make_unique<default_retry_policy>()) {}

ss::future<> client::shutdown() {
    auto gate_closed = _gate.close();
    co_await _http_client->shutdown_and_stop();
    co_await std::move(gate_closed);
}

expected<ss::gate::holder> client::maybe_gate() {
    if (_gate.is_closed()) {
        return std::unexpected(
          domain_error{aborted_error{"client gate is closed"}});
    }
    return _gate.hold();
}

void client::maybe_add_basic_auth(http::request_builder& request) {
    if (_credentials.has_value()) {
        request.with_basic_auth(_credentials->username, _credentials->password);
    }
}

ss::future<expected<iobuf>> client::perform_request(
  retry_chain_node& parent_rtc,
  http::request_builder builder,
  std::optional<iobuf> payload) {
    if (payload.has_value()) {
        builder.with_content_length(payload.value().size_bytes());
    }
    retry_chain_node rtc(&parent_rtc);
    std::vector<error_kind> retriable_errors;
    std::optional<http_call_error> last_error;

    while (true) {
        retry_permit permit{};
        try {
            permit = rtc.retry();
        } catch (...) {
            auto ex = std::current_exception();
            auto msg = fmt::format("{}", ex);
            if (ssx::is_shutdown_exception(ex)) {
                vlog(srlog.debug, "shutting down during request: {}", msg);
                co_return std::unexpected(domain_error{aborted_error{msg}});
            }
            // We only expect shutdown exceptions here; treat anything else
            // conservatively as exhausted rather than aborted.
            vlog(srlog.warn, "schema registry request gave up: {}", msg);
            co_return std::unexpected(
              domain_error{retries_exhausted{
                .reasons = std::move(retriable_errors),
                .last_error = std::move(last_error)}});
        }
        if (!permit.is_allowed) {
            co_return std::unexpected(
              domain_error{retries_exhausted{
                .reasons = std::move(retriable_errors),
                .last_error = std::move(last_error)}});
        }

        auto request = builder.host(_endpoint).build();
        if (!request.has_value()) {
            co_return std::unexpected(domain_error{request.error()});
        }

        auto response_f = co_await ss::coroutine::as_future(
          _http_client->request_and_collect_response(
            std::move(request.value()),
            payload.has_value() ? std::make_optional(payload->copy())
                                : std::nullopt));

        auto call_res = _retry_policy->should_retry(std::move(response_f));
        if (call_res.has_value()) {
            co_return std::move(call_res->body);
        }

        auto& error = call_res.error();
        if (error.kind == error_kind::aborted) {
            co_return std::unexpected(
              domain_error{
                aborted_error{"shutting down while evaluating retry"}});
        }
        if (!is_retriable(error.kind)) {
            vlog(srlog.warn, "schema registry request failed: {}", error.err);
            co_return std::unexpected(
              co_await attach_error_code(domain_error{std::move(error.err)}));
        }

        retriable_errors.push_back(error.kind);
        last_error.emplace(std::move(error.err));
        auto sleep_fut = co_await ss::coroutine::as_future(
          ss::sleep_abortable(permit.delay, rtc.root_abort_source()));
        if (sleep_fut.failed()) {
            auto msg = fmt::format(
              "exception during retry sleep: {}", sleep_fut.get_exception());
            vlog(srlog.debug, "{}", msg);
            co_return std::unexpected(domain_error{aborted_error{msg}});
        }
    }
}

ss::future<expected<chunked_vector<context_subject>>>
client::list_subjects(retry_chain_node& rtc) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return std::unexpected(std::move(gate.error()));
    }
    // TODO: support query params for pagination (offset/limit) and filtering
    // (deleted/deletedOnly, subjectPrefix); v1 lists all live subjects across
    // all contexts.
    auto request = http::request_builder{}
                     .method(boost::beast::http::verb::get)
                     .path("/subjects")
                     .header("accept", accept_json);
    maybe_add_basic_auth(request);

    auto response = co_await perform_request(rtc, std::move(request));
    if (!response.has_value()) {
        co_return std::unexpected(std::move(response.error()));
    }
    auto parsed = co_await parse_subjects(
      std::move(response.value()), _qualified);
    if (!parsed.has_value()) {
        co_return std::unexpected(domain_error{std::move(parsed.error())});
    }
    co_return std::move(parsed.value());
}

} // namespace pandaproxy::schema_registry::rest_client
