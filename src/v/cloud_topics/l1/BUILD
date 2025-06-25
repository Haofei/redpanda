load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/l1:__subpackages__"])

redpanda_cc_library(
    name = "state",
    srcs = [
        "state.cc",
    ],
    hdrs = [
        "state.h",
    ],
    include_prefix = "cloud_topics/l1",
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:set",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "//src/v/serde:named_type",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
