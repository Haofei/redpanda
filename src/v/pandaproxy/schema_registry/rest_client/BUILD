load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/pandaproxy:__subpackages__"])

redpanda_cc_library(
    name = "parse",
    srcs = [
        "parse.cc",
    ],
    hdrs = [
        "parse.h",
    ],
    implementation_deps = [
        "//src/v/serde/json:parser",
        "//src/v/ssx:sformat",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/pandaproxy/schema_registry:types",
        "@seastar",
    ],
)
