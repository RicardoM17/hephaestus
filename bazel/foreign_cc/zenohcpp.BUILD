# =================================================================================================
# Copyright (C) 2023-2024 HEPHAESTUS Contributors
# =================================================================================================

cc_library(
    name = "zenoh-cpp",
    hdrs = glob([
        "include/**/*.hxx",
    ]),
    includes = ["include"],
    visibility = ["//visibility:public"],
    deps = ["@zenohc_builder//:zenoh-c"],
)
