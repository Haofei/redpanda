load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "source_reader",
    srcs = [
        "unavailable_source_reader.cc",
    ],
    hdrs = [
        "source_reader.h",
        "unavailable_source_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)
