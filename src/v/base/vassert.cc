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
#include "base/vassert.h"

#include "base/seastarx.h"
#include "base/vassert-register.h"

#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <atomic>
#include <string_view>

namespace detail {
using namespace base;
struct dummyassert {
    static inline ss::logger l{"assert"};
};
inline dummyassert g_assert_log;
/**
 * @brief Class used to format assert messages
 *
 * This class will format the provided assert message and produce a backtrace
 * caused by an assert.  It also provides a means of registering a callback
 * function that will be called when an assert is triggered.
 */
class assert_log_holder {
public:
    void do_assert(
      ss::saved_backtrace bt,
      const char* prefix, // NOLINT(bugprone-easily-swappable-parameters)
      const char* format,
      fmt::format_args args) noexcept {
        std::string buffer = fmt::format(
          "Assert failure: {} {}", prefix, fmt::vformat(format, args));
        assert_handler(bt, buffer);
    }

    void register_cb(assert_cb_func cb) {
        assert_cb_func before = nullptr;
        _cb_func.compare_exchange_strong(before, cb);
    }

private:
    friend void ::base::register_event(
      const ss::saved_backtrace&, std::string_view);

    void assert_handler(ss::saved_backtrace bt, std::string_view text) {
        g_assert_log.l.error("{}", text);
        g_assert_log.l.error("Backtrace:\n{}", bt);

        auto cb_func = _cb_func.load();
        if (cb_func != nullptr) {
            cb_func(text);
        }
    }

    std::atomic<assert_cb_func> _cb_func{nullptr};
};

assert_log_holder g_assert_log_holder;

// Implementation notes:
// Asserts rarely trigger: after all, they can occur at most once
// during the lifetime of the program! So the priorities are,
// in no specific order, something like:
// 1) Avoid slowing down the fast path (no assert) in terms of
//    instructions, i.e. "micro" execution
// 2) Avoid polluting the hot path icache lines with cold code that
//    is rarely going to be executed, i.e., the assertion
//    handling code
// 3) Avoid duplicating assertion handling code in every TU, which
//    slows down compile time and wastes disk space (and in the shared
//    library build space in the final binaries)
//
// To do (1) we note that both clang and gcc are pretty bad at keeping
// the hot path clean, even when the cold path is clearly hidden behind an
// [[unlikely]] annotation. In particular, in the hot path it will spill locals
// that are passed by reference to an opaque function in the cold part, and also
// insert stack canary checks (we use -fstack-protector-strong) in the hot path
// even though none of that is needed in the hot path.
//
// See the following bugs for examples directly derived from this case:
//
// https://github.com/llvm/llvm-project/issues/129750
// https://github.com/llvm/llvm-project/issues/129748
//
// In order to minimize junk on the hot path we should try to avoid the address
// of local variables escaping out of the inlined function where the original
// vassert call occur. Calls to fmt::format pass locals will generally escape
// them because: (a) it holds most arguments by void *, and (b) even for
// primitives, which it does hold by value, the size of the format_args
// structure quickly exceeds 16 bytes which means it will be passed on the stack
// anyway, which escapes the address of that object causing similar problems.
//
// So the approach is to use several things in the cold path: the thunk0 takes
// all fmt arguments by reference and will be inlined (so doesn't cause
// escaping) and decides on a per-argument basis whether to pass by value or
// reference (passing trivially constructible values by value) and calls thunk1
// with all arguments passed in the selected way. Thunk0 ends up in the calling
// function but "out of line" (as it is called on an [[unlikely]] branch. Then
// thunk1 is a template function depending on all the argument types but cold
// and so not inlined and compiled into the text.unlikely section (helping with
// goal 2). This thunk uses make::format_args which erases the arguments and
// calls thunk2 which is not a template and can be implemented entirely in the
// .cc.
//
// For many simple calls this results in a good hot path with no junk from the
// cold path. The hot path will still get junk if format arguments need to be
// passed by reference.
//
// This goldbolt link may be useful when considering changes to this
// strategy: https://godbolt.org/z/naYddPff5

// This thunk is fully type erased. See implementation details above.
[[gnu::cold]] [[noreturn]]
void assert_failed_thunk2(
  const char* prefix, const char* msg, fmt::format_args args) noexcept {
    ::detail::g_assert_log_holder.do_assert(
      ss::current_backtrace(), prefix, msg, args);
    __builtin_trap();
}

} // namespace detail

using namespace detail;

namespace base {

void register_event(const ss::saved_backtrace& bt, std::string_view message) {
    g_assert_log_holder.assert_handler(bt, message);
}

void register_cb(assert_cb_func cb) { g_assert_log_holder.register_cb(cb); }

} // namespace base
