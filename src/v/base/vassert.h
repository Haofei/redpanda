/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/likely.h"
#include "base/seastarx.h"

#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <atomic>
#include <string_view>

namespace detail {
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
    // We want to enforce using a non-capturing function for callbacks to ensure
    // a static lifetime for the callback as we will not permit unregistering
    // a callback to prevent a race condition between unregistering the callback
    // on one thread and having the callback called by a different thread.
    using assert_cb_func = void (*)(std::string_view);

    /**
     * @brief Registers a vassert event
     *
     * @tparam Args The argument template
     * @param bt The backtrace from the assert
     * @param fmt The format string for the log
     * @param args The arguments to @p fmt
     */
    template<typename... Args>
    void register_event(
      const ss::saved_backtrace& bt,
      ss::logger::format_info_t<Args...> fmt,
      Args&&... args) {
        auto text = fmt::format(
          fmt::runtime(fmt.format), std::forward<Args>(args)...);
        assert_handler(bt, text);
    }

    /**
     * @brief Registers a vassert event
     *
     * @tparam Args The argument template
     * @param bt The backtrace from the assert
     * @param fmt The format string for the log
     * @param args The arguments to @p fmt
     */
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
inline assert_log_holder g_assert_log_holder;

// Implementation notes:
// Asserts rarely trigger: after all, they can occur at most once
// during the lifetime of the program! So the priorities are,
// in no specific order something like:
// 1) Avoid slowing down the fast path (no assert) in terms of
//    instructions, "micro" execution
// 2) Avoid polluting the hot icache lines with cold code that
//    is rarely going to be executed, i.e., the assertion
//    handling code
// 3) Avoid duplicating assertion handling code in every TU, which
//    slows down compile time and wastes space.
//
// To do (1) we node that both clang and gcc is pretty bad at keeping
// the hot path clean, even when the cold path is clearly hidden behind an
// [[unlikely]] annotation. In particular, it will spill locals that are passed
// by reference in the cold part, and also insert stack canary checks (we use
// -fstack-protector-strong) in the hot path even though none of that is needed
// here. See the following bugs for examples directly derived from this case:
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
// This godbolt link may be useful when considering changes to this
// strategy: https://godbolt.org/z/naYddPff5

// Pass by value for 16-byte values which are trivially copyable.
// This captures most/all of the values which can be passed in registers,
// and avoids large copies. The condition here only affects binary size
// and performance, not correctness: it could be hardcoded to false or
// true and still be correct.
template<typename T>
constexpr bool pass_by_value = std::is_trivially_copy_constructible_v<T>
                               && sizeof(T) <= 16;

template<typename T, typename T_ = std::remove_cvref_t<T>>
using fwd_type = std::conditional_t<pass_by_value<T_>, T_, const T&>;

// This thunk is fully type erased. See implementation details in vassert.cc.
[[gnu::cold]] [[noreturn]]
inline void assert_failed_thunk2(
  const char* prefix, const char* msg, fmt::format_args args) {
    ::detail::g_assert_log_holder.do_assert(
      ss::current_backtrace(), prefix, msg, args);
    __builtin_trap();
}

// This thunk accepts all the format arguments using the selected passing method
// and erases them. It is cold so appears in the cold section of the binary.
template<typename... Args>
[[gnu::cold]] [[noreturn]] [[gnu::noinline]]
void assert_failed_thunk1(
  const char* prefix, const char* msg, Args... args) noexcept {
    assert_failed_thunk2(prefix, msg, fmt::make_format_args(args...));
}

// This thunk will be inlined into the calling function and is responsible for
// dispatching. We need the always_inline since otherwise the noreturn attribute
// causes clang to outline it.
template<typename... Args>
[[noreturn]] [[gnu::always_inline]]
inline void assert_failed_thunk0(
  const char* prefix, const char* msg, const Args&... args) noexcept {
    ::detail::assert_failed_thunk1<fwd_type<Args>...>(prefix, msg, args...);
}

} // namespace detail

// helpers to turn __LINE__ into a string literal
#define STR_VASSERT2(x) #x
#define STR_VASSERT(x) STR_VASSERT2(x)

/** Meant to be used in the same way as assert(condition, msg);
 * which means we use the negative conditional.
 * i.e.:
 *
 * open_fileset::~open_fileset() noexcept {
 *   vassert(_closed, "fileset not closed");
 * }
 *
 */
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define vassert(x, msg, args...)                                               \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        /*The !(x) is not an error. see description above*/                    \
        if (unlikely(!(x))) {                                                  \
            ::detail::assert_failed_thunk0(                                    \
              "(" __FILE__ ":" STR_VASSERT(__LINE__) ") '" #x "'",             \
              msg,                                                             \
              ##args);                                                         \
        }                                                                      \
    } while (0)

/**
 * same as vassert but only debug mode. Use over assert for better
 * error messages.
 */
#ifndef NDEBUG
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define dassert(x, msg, args...) vassert(x, msg, ##args)
#else
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define dassert(...)
#endif
