load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "state_test",
    timeout = "short",
    srcs = [
        "state_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics/l1:state",
        "//src/v/serde",
        "//src/v/serde:uuid",
        "//src/v/serde:named_type",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

