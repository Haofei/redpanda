// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/single_fiber_executor.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace {

using fn_t = ss::noncopyable_function<ss::future<ss::sstring>(
  ss::abort_source&) noexcept(false)>;

fn_t make_ready_coro(ss::sstring retval) {
    return {[retval](ss::abort_source&) { return ssx::now(retval); }};
}

fn_t make_slow_coro(ss::semaphore& sem, ss::sstring retval) {
    return {[&sem, retval](ss::abort_source& as) {
        return sem.wait(as).then([retval] { return retval; });
    }};
}

fn_t make_uncooperative_coro(ss::semaphore& sem, ss::sstring retval) {
    return {[&sem, retval](ss::abort_source& as) {
        return sem.wait().then([&as, retval] {
            return as.abort_requested() ? ss::sstring("tripped-on-abort-source")
                                        : retval;
        });
    }};
}

fn_t make_side_effect_coro(
  ss::semaphore& sem, ss::sstring retval, int& counter) {
    return {[&sem, retval, &counter](ss::abort_source& as) {
        return sem.wait().then([&as, retval, &counter] {
            ++counter;
            return as.abort_requested() ? ss::sstring("tripped-on-abort-source")
                                        : retval;
        });
    }};
}

using sfe = ssx::single_fiber_executor<fn_t>;

// depending on Seastar scheduling, the coro may or may not complete
MATCHER_P(InterruptedOr, expected_value, "") {
    return arg ? arg.value() == expected_value
               : arg.error() == sfe::errc::interrupted;
}

MATCHER_P(CompletedAnd, expected_value, "") {
    return arg && arg.value() == expected_value;
}

constexpr std::expected<ss::sstring, sfe::errc> interrupted{
  std::unexpect, sfe::errc::interrupted};

void assure_new_tasks_are_bounced(ssx::single_fiber_executor<fn_t>& executor) {
    auto f = executor.submit(make_ready_coro("na"));
    ASSERT_EQ(f.get(), interrupted);
}

} // namespace

TEST(SingleFiberExecutor, no_destruction_test) {
    ssx::single_fiber_executor<fn_t> executor;

    { // superseded ones may or may not complete
        auto f1 = executor.submit(make_ready_coro("ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        auto f3 = executor.submit(make_ready_coro("ret3"));
        ASSERT_THAT(f1.get(), InterruptedOr("ret1"));
        ASSERT_THAT(f2.get(), InterruptedOr("ret2"));
        ASSERT_THAT(f3.get(), CompletedAnd("ret3"));
    }

    { // running, superseded and queued -- all get aborted
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_slow_coro(sem, "ret2"));
        auto f3 = executor.submit(make_slow_coro(sem, "ret3"));
        executor.request_abort();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_EQ(f2.get(), interrupted);
        ASSERT_EQ(f3.get(), interrupted);
    }

    { // the second is scheduled before the first one completes
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        sem.signal();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }

    { // the result of the interrupted one is discarded
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_uncooperative_coro(sem, "ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        sem.signal();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }

    { // aborted yields to next scheduled
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        executor.request_abort();
        auto f2 = executor.submit(make_ready_coro("ret2"));
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }

    { // latest scheduled runs to completion
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_slow_coro(sem, "ret2"));
        auto f3 = executor.submit(make_slow_coro(sem, "ret3"));
        sem.signal(3);
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_EQ(f2.get(), interrupted);
        ASSERT_THAT(f3.get(), CompletedAnd("ret3"));
    }

    { // exception gets propagated
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        sem.broken();
        ASSERT_THROW(f1.get(), ss::broken_semaphore);
    }

    { // exception gets propagated for the pending one as well
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_slow_coro(sem, "ret2"));
        sem.broken();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THROW(f2.get(), ss::broken_semaphore);
    }

    { // superseding wins over exception
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        sem.broken();
        ASSERT_EQ(f1.get(), interrupted);
        ASSERT_THAT(f2.get(), CompletedAnd("ret2"));
    }
}

TEST(SingleFiberExecutor, drain_test) {
    { // drain while idle
        ssx::single_fiber_executor<fn_t> executor;
        auto drain_f = executor.drain();
        assure_new_tasks_are_bounced(executor);
        drain_f.get();
        assure_new_tasks_are_bounced(executor);
    }

    { // drain while running
        ssx::single_fiber_executor<fn_t> executor;
        ss::semaphore sem{0};
        auto f1 = executor.submit(make_slow_coro(sem, "ret1"));
        auto drain_f = executor.drain();
        ASSERT_EQ(f1.get(), interrupted); // it got canceled by drain
        assure_new_tasks_are_bounced(executor);
        drain_f.get();
        assure_new_tasks_are_bounced(executor);
    }

    { // drain while running and queued
        ssx::single_fiber_executor<fn_t> executor;
        ss::semaphore sem{0};
        int cnt_f1_completed = 0;
        auto f1 = executor.submit(
          make_side_effect_coro(sem, "ret1", cnt_f1_completed));
        auto f2 = executor.submit(make_ready_coro("ret2"));
        auto drain_f = executor.drain();
        ASSERT_EQ(f1.get(), interrupted); // it got canceled by drain
        ASSERT_EQ(f2.get(), interrupted); // and this one never even got run
        ASSERT_EQ(cnt_f1_completed, 0);   // however, f1 hasn't completed
        assure_new_tasks_are_bounced(executor);
        ASSERT_FALSE(drain_f.available()); // can't complete because f1 is stuck
        sem.signal();                      // let f1 complete
        drain_f.get();
        ASSERT_EQ(cnt_f1_completed, 1); // it has completed now
        assure_new_tasks_are_bounced(executor);
    }
    return;
}

TEST(SingleFiberExecutor, empty_return_test) {
    using fn_t = ss::noncopyable_function<ss::future<>(
      ss::abort_source&) noexcept(false)>;
    using sfe = ssx::single_fiber_executor<fn_t>;
    auto make_ready_coro = [] -> fn_t {
        return {[](ss::abort_source&) -> ss::future<> { return ss::now(); }};
    };
    constexpr std::expected<void, sfe::errc> completed{};

    ssx::single_fiber_executor<fn_t> executor;
    { // superseded ones may or may not complete
        auto f1 = executor.submit(make_ready_coro());
        auto f2 = executor.submit(make_ready_coro());
        auto f3 = executor.submit(make_ready_coro());
        f1.get();
        f2.get();
        ASSERT_EQ(f3.get(), completed);
    }
    auto drain_f = executor.drain();
    drain_f.get();
}

TEST(SingleFiberExecutor, lvalue_test) {
    using sfe = ssx::single_fiber_executor<fn_t&>;
    constexpr std::expected<ss::sstring, sfe::errc> interrupted{
      std::unexpect, sfe::errc::interrupted};

    ssx::single_fiber_executor<fn_t&> executor;
    fn_t f = make_ready_coro("ret1");
    { // superseded ones may or may not complete
        auto f1 = executor.submit(f);
        auto f2 = executor.submit(f);
        auto f3 = executor.submit(f);
        ASSERT_THAT(f1.get(), InterruptedOr("ret1"));
        ASSERT_THAT(f2.get(), InterruptedOr("ret1"));
        ASSERT_THAT(f3.get(), CompletedAnd("ret1"));
    }
    auto drain_f = executor.drain();
    drain_f.get();
}

namespace {
constexpr auto make_fn(int retval) {
    return [retval](ss::abort_source&) { return ssx::now(retval); };
}
} // namespace

TEST(SingleFiberExecutor, constexpr_test) {
    // this test demonstrates how to use it with a lambda factory without a
    // type-erasing wrapper

    // rvalue
    ssx::single_fiber_executor<decltype(make_fn(0))> executor_r;
    auto f1 = executor_r.submit(make_fn(1));
    ASSERT_EQ(f1.get(), 1);

    // lvalue
    ssx::single_fiber_executor<decltype(make_fn(0))&> executor_l;
    auto fn = make_fn(5);
    auto f2 = executor_l.submit(fn);
    ASSERT_EQ(f2.get(), 5);
}
