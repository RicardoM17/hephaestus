//=================================================================================================
// Copyright (C) 2023-2024 HEPHAESTUS Contributors
//=================================================================================================

#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include <fmt/format.h>

namespace heph::examples::types {

struct SampleRequest {
  std::size_t initial_value{ 0 };
  std::size_t iterations_count{ 0 };
};

struct SampleReply {
  std::size_t value{ 0 };
  std::size_t counter{ 0 };
};

struct StressMessage {
  std::size_t counter{ 0 };
  std::vector<float> data{};
};

// NOLINTNEXTLINE(readability-identifier-naming)
static inline auto format_as(const SampleRequest& sample) -> std::string {
  return fmt::format("initial value: {} | iterations: {}", sample.initial_value, sample.iterations_count);
}

// NOLINTNEXTLINE(readability-identifier-naming)
static inline auto format_as(const SampleReply& sample) -> std::string {
  return fmt::format("value: {} | counter: {}", sample.value, sample.counter);
}

// NOLINTNEXTLINE(readability-identifier-naming)
static inline auto format_as(const StressMessage& sample) -> std::string {
  return fmt::format("counter: {} | data size: {}", sample.counter, sample.data.size());
}

}  // namespace heph::examples::types
