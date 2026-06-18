load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "parse_test",
    timeout = "short",
    srcs = [
        "parse_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/pandaproxy/schema_registry/rest_client:parse",
        "//src/v/ssx:sformat",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
