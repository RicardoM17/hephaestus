# =================================================================================================
# Copyright (C) 2023-2024 HEPHAESTUS Contributors
# =================================================================================================

cc_library(
    name = "zenoh-c",
    srcs = ["lib/libzenohc.a"],
    hdrs = glob(["include/**/*.h"]),
    defines = [
        "ZENOH_WITH_UNSTABLE_API",
        "Z_FEATURE_UNSTABLE_API",
        "ZENOHCXX_ZENOHC",
    ],
    includes = ["include"],
    visibility = ["//visibility:public"],
)

# For backward compatibility, create an alias
cc_library(
    name = "zenoh-cpp",
    visibility = ["//visibility:public"],
    deps = ["@zenohcpp//:zenoh-cpp"],
)
