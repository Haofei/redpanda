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

#include "container/priority_queue.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <queue>
#include <vector>

struct value {
    explicit value(int data)
      : _data(data) {}

    auto operator<=>(const value&) const = default;
    bool operator==(const value&) const = default;

    int _data;
};

struct move_only {
    explicit move_only(int data)
      : _data{data} {}

    move_only(move_only&&) = default;
    move_only& operator=(move_only&&) = default;

    move_only(const move_only&) = delete;
    move_only& operator=(const move_only&) = delete;

    ~move_only() = default;

    auto operator<=>(const move_only&) const = default;
    bool operator==(const move_only&) const = default;

    int _data;
};

template<template<typename...> class Queue, typename ValueType>
struct QueueFactory {
    // Use large capacity for bounded queues so they behave like unbounded for
    // small test data
    static constexpr size_t large_capacity = 1000;
    using value_type = ValueType;

    static auto make_queue() { return Queue<ValueType>{}; }

    template<typename Comp>
    static auto make_queue_with_comp(Comp comp) {
        return Queue<ValueType, Comp>{comp};
    }

    static value_type make_value(int val) { return value_type{val}; }
};

// Specific factory types using the generic template
template<typename T>
using UnboundedQueueFactory = QueueFactory<chunked_priority_queue, T>;

// Types to test - both value types and move-only types
using QueueTypes = ::testing::
  Types<UnboundedQueueFactory<int>, UnboundedQueueFactory<move_only>>;

// Copyable types only (for tests that require copying)
using CopyableQueueTypes = ::testing::Types<UnboundedQueueFactory<int>>;

// Types for std compatibility testing (unbounded only, copyable types)
using StdCompatibilityTypes
  = ::testing::Types<UnboundedQueueFactory<int>, UnboundedQueueFactory<value>>;

// Typed test suite for std::priority_queue compatibility - inherits common
// functionality
template<typename QueueFactory>
class StdCompatibilityTest
  : public QueueFactory
  , public ::testing::Test {};

TYPED_TEST_SUITE(StdCompatibilityTest, StdCompatibilityTypes);

// Test drop-in replacement compatibility with std::priority_queue
TYPED_TEST(StdCompatibilityTest, StdCompatibility) {
    std::priority_queue<typename TestFixture::value_type> std_pq;
    priority_queue<typename TestFixture::value_type> our_pq;

    for (int i : {3, 1, 4, 1, 5}) {
        std_pq.push(this->make_value(i));
        our_pq.push(this->make_value(i));
    }

    // Test size and empty
    EXPECT_EQ(std_pq.size(), our_pq.size());
    EXPECT_EQ(std_pq.empty(), our_pq.empty());

    // Test top and pop equivalence
    while (!std_pq.empty() && !our_pq.empty()) {
        EXPECT_EQ(std_pq.top(), our_pq.top());
        std_pq.pop();
        std::ignore = our_pq.pop();
    }

    EXPECT_EQ(std_pq.empty(), our_pq.empty());
}

// Typed test suite for common functionality
template<typename QueueFactory>
class PriorityQueueCommonTest
  : public QueueFactory
  , public ::testing::Test {};

TYPED_TEST_SUITE(PriorityQueueCommonTest, QueueTypes);

TYPED_TEST(PriorityQueueCommonTest, BasicOperations) {
    auto pq = this->make_queue();

    EXPECT_TRUE(pq.empty());
    EXPECT_EQ(pq.size(), 0);

    auto test_value = this->make_value(42);
    pq.push(std::move(test_value));
    EXPECT_FALSE(pq.empty());
    EXPECT_EQ(pq.size(), 1);

    auto expected_value = this->make_value(42);
    EXPECT_EQ(pq.top(), expected_value);

    std::ignore = pq.pop();
    EXPECT_TRUE(pq.empty());
}

TYPED_TEST(PriorityQueueCommonTest, PushRange) {
    auto pq = this->make_queue();

    // Create a vector of values of the appropriate type
    {
        std::vector<typename TestFixture::value_type> values;
        values.push_back(this->make_value(3));
        pq.push_range(std::move(values));
    }

    {
        std::vector<typename TestFixture::value_type> values;
        for (int val : {1, 4, 1, 5}) {
            values.push_back(this->make_value(val));
        }
        pq.push_range(std::move(values));
    }
    EXPECT_EQ(pq.size(), 5);

    std::vector<typename TestFixture::value_type> results;
    while (!pq.empty()) {
        results.push_back(pq.pop());
    }

    EXPECT_TRUE(std::ranges::is_sorted(results, std::ranges::greater{}));
}

TYPED_TEST(PriorityQueueCommonTest, ExtractHeap) {
    auto pq = this->make_queue();

    std::vector<typename TestFixture::value_type> values;
    for (int val : {3, 1, 4, 1, 5}) {
        values.push_back(this->make_value(val));
    }
    pq.push_range(std::move(values));

    auto heap = std::move(pq).extract_heap();
    EXPECT_TRUE(std::ranges::is_heap(heap));

    constexpr auto pop = [](auto& heap) {
        std::ranges::pop_heap(heap);
        auto val = std::move(heap.back());
        heap.pop_back();
        return val;
    };

    EXPECT_EQ(pop(heap), this->make_value(5));
    EXPECT_EQ(pop(heap), this->make_value(4));
    EXPECT_EQ(pop(heap), this->make_value(3));
    EXPECT_EQ(pop(heap), this->make_value(1));
    EXPECT_EQ(pop(heap), this->make_value(1));
}

TYPED_TEST(PriorityQueueCommonTest, ExtractSorted) {
    auto pq = this->make_queue();

    std::vector<typename TestFixture::value_type> values;
    for (int val : {3, 1, 4, 1, 5}) {
        values.push_back(this->make_value(val));
    }
    pq.push_range(std::move(values));

    auto sorted = std::move(pq).extract_sorted();

    EXPECT_EQ(sorted.size(), 5);
    EXPECT_EQ(sorted[0], this->make_value(1));
    EXPECT_EQ(sorted[1], this->make_value(1));
    EXPECT_EQ(sorted[2], this->make_value(3));
    EXPECT_EQ(sorted[3], this->make_value(4));
    EXPECT_EQ(sorted[4], this->make_value(5));
}

TYPED_TEST(PriorityQueueCommonTest, CustomComparator) {
    auto pq = this->make_queue_with_comp(std::ranges::greater{});

    // Create a vector of values with custom comparator (min-heap behavior)
    std::vector<typename TestFixture::value_type> values;
    for (int val : {3, 1, 4, 1, 5}) {
        values.push_back(this->make_value(val));
    }

    pq.push_range(std::move(values));
    EXPECT_EQ(pq.size(), 5);

    std::vector<typename TestFixture::value_type> sorted;
    while (!pq.empty()) {
        sorted.push_back(pq.pop());
    }

    EXPECT_EQ(sorted.size(), 5);
    EXPECT_EQ(sorted[0], this->make_value(1));
    EXPECT_EQ(sorted[1], this->make_value(1));
    EXPECT_EQ(sorted[2], this->make_value(3));
    EXPECT_EQ(sorted[3], this->make_value(4));
    EXPECT_EQ(sorted[4], this->make_value(5));
}
