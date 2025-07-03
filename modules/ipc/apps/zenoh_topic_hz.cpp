//=================================================================================================
// Copyright (C) 2023-2024 HEPHAESTUS Contributors
//=================================================================================================

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>

#include "hephaestus/cli/program_options.h"
#include "hephaestus/ipc/topic_filter.h"
#include "hephaestus/ipc/zenoh/dynamic_subscriber.h"
#include "hephaestus/ipc/zenoh/program_options.h"
#include "hephaestus/ipc/zenoh/raw_subscriber.h"
#include "hephaestus/ipc/zenoh/session.h"
#include "hephaestus/serdes/dynamic_deserializer.h"
#include "hephaestus/serdes/type_info.h"
#include "hephaestus/telemetry/log.h"
#include "hephaestus/telemetry/log_sinks/absl_sink.h"
#include "hephaestus/utils/exception.h"
#include "hephaestus/utils/signal_handler.h"
#include "hephaestus/utils/stack_trace.h"

namespace
{
constexpr auto DEFAULT_WINDOW_SIZE_SECONDS = 10;
constexpr auto DEFAULT_UPDATE_INTERVAL_SECONDS = 5;

struct TopicStats
{
  std::vector<std::chrono::steady_clock::time_point> message_times;
  std::size_t total_messages = 0;
  std::chrono::steady_clock::time_point first_message_time;
  std::chrono::steady_clock::time_point last_message_time;
  std::chrono::steady_clock::time_point node_start_time;
};
}  // namespace

namespace heph::ipc::zenoh::apps
{
class TopicHz
{
public:
  TopicHz(SessionPtr session, TopicFilterParams topic_filter_params, std::chrono::seconds window_size,
          std::chrono::seconds update_interval)
    : window_size_(window_size), update_interval_(update_interval), node_start_time_(std::chrono::steady_clock::now())
  {
    // Force immediate flushing for Docker logging - applies globally
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    DynamicSubscriberParams params{ .session = std::move(session),
                                    .topics_filter_params = std::move(topic_filter_params),
                                    .init_subscriber_cb =
                                        [this](const auto& topic, const auto& type_info) {
                                          (void)type_info;
                                          initTopicStats(topic);
                                        },
                                    .subscriber_cb =
                                        [this](const auto& metadata, auto data, const auto& topic_info) {
                                          (void)data;
                                          (void)topic_info;
                                          recordMessage(metadata.topic);
                                        } };

    dynamic_subscriber_ = std::make_unique<DynamicSubscriber>(std::move(params));
  }

  [[nodiscard]] auto start() -> std::future<void>
  {
    auto future = dynamic_subscriber_->start();

    // Start the stats reporting thread
    stats_thread_ = std::thread([this]() {
      while (!stop_requested_)
      {
        std::this_thread::sleep_for(update_interval_);
        if (!stop_requested_)
        {
          printStats();
        }
      }
    });

    return future;
  }

  [[nodiscard]] auto stop() -> std::future<void>
  {
    stop_requested_ = true;
    if (stats_thread_.joinable())
    {
      stats_thread_.join();
    }
    return dynamic_subscriber_->stop();
  }

private:
  void initTopicStats(const std::string& topic)
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    if (topic_stats_.find(topic) == topic_stats_.end())
    {
      topic_stats_[topic] = TopicStats{};
      topic_stats_[topic].node_start_time = node_start_time_;
      fmt::println("Monitoring topic: {}", topic);
    }
  }

  void recordMessage(const std::string& topic)
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto now = std::chrono::steady_clock::now();

    auto& stats = topic_stats_[topic];
    stats.total_messages++;
    stats.last_message_time = now;

    if (stats.total_messages == 1)
    {
      stats.first_message_time = now;
    }

    // Add message time to rolling window
    stats.message_times.push_back(now);

    // Remove old messages outside the window
    auto cutoff_time = now - window_size_;
    stats.message_times.erase(std::remove_if(stats.message_times.begin(), stats.message_times.end(),
                                             [cutoff_time](const auto& time) { return time < cutoff_time; }),
                              stats.message_times.end());
  }

  void printStats()
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto now = std::chrono::steady_clock::now();

    // Clear screen for live updates
    fmt::print("\033[2J\033[H");

    fmt::println("=== Topic Frequency Statistics (Window: {}s) ===", window_size_.count());
    fmt::println("Timestamp: {}", std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
    fmt::println("");

    if (topic_stats_.empty())
    {
      fmt::println("No topics discovered yet...");
      return;
    }

    fmt::println("{:<40} {:<15} {:<15} {:<15} {:<15} {:<15}", "Topic", "Window Hz", "Total Msgs", "Runtime Hz",
                 "Target Hz", "Target Ratio");
    fmt::println("{:-<105}", "");

    double sum_frequency_ratios = 0.0;
    int valid_ratios = 0;

    for (const auto& [topic, stats] : topic_stats_)
    {
      double window_freq = 0.0;
      double runtime_freq = 0.0;
      double target_ratio = 0.0;

      // Calculate frequency in current window
      if (stats.message_times.size() > 1)
      {
        auto window_duration = std::chrono::duration_cast<std::chrono::duration<double>>(
            std::min(window_size_, std::chrono::duration_cast<std::chrono::seconds>(now - stats.first_message_time)));
        if (window_duration.count() > 0)
        {
          window_freq = static_cast<double>(stats.message_times.size()) / window_duration.count();
        }
      }

      // Calculate frequency across entire runtime
      if (stats.total_messages > 0)
      {
        auto runtime_duration = std::chrono::duration_cast<std::chrono::duration<double>>(now - stats.node_start_time);
        if (runtime_duration.count() > 0)
        {
          runtime_freq = static_cast<double>(stats.total_messages) / runtime_duration.count();
        }
      }

      // Calculate target ratio (placeholder - will be implemented based on environment variables)
      double target_topic_rate = getTargetTopicRate(topic);
      if (target_topic_rate > 0.0)
      {
        target_ratio = runtime_freq / target_topic_rate;
        sum_frequency_ratios += target_ratio;
        valid_ratios++;
      }

      fmt::println("{:<40} {:<15.2f} {:<15} {:<15.2f} {:<15.2f} {:<15.2f}", topic, window_freq, stats.total_messages,
                   runtime_freq, target_topic_rate, target_ratio);
    }

    // Calculate and display average frequency target
    if (valid_ratios > 0)
    {
      double average_frequency_target = sum_frequency_ratios / valid_ratios;
      fmt::println("");
      fmt::println("Average Frequency Target: {:.3f} ({} topics)", average_frequency_target, valid_ratios);
    }

    fmt::println("");
    fmt::println("Press Ctrl+C to stop monitoring...");
  }

  // Function to determine target rate for a topic based on environment variables and topic naming
  double getTargetTopicRate(const std::string& topic)
  {
    // Topic naming pattern: "hephaestus/ipc/example/zenoh/put_<topic_id>"
    // where topic_id = node_id * num_topics + publisher_index
    //
    // Rate assignment logic (from zenoh_pub_sub.cpp):
    // - High rate (100 Hz) for publisher indices 0, 1 when HIGH_RATE=true
    // - Medium rate (20 Hz) for publisher indices 2, 3 when MEDIUM_RATE=true
    // - Default rate (5 Hz) for all others

    // Extract topic_id from topic name
    auto pos = topic.find_last_of('_');
    if (pos == std::string::npos)
    {
      return 5.0;  // Default rate fallback
    }

    try
    {
      int topic_id = std::stoi(topic.substr(pos + 1));

      // Get environment variables
      const char* num_topics_env = std::getenv("NUM_TOPICS");
      const char* high_rate_env = std::getenv("HIGH_RATE");
      const char* medium_rate_env = std::getenv("MEDIUM_RATE");

      int num_topics = num_topics_env ? std::atoi(num_topics_env) : 3;
      bool high_rate_enabled = high_rate_env && (std::string(high_rate_env) == "true");
      bool medium_rate_enabled = medium_rate_env && (std::string(medium_rate_env) == "true");

      // Rate constants from zenoh_pub_sub.cpp
      constexpr double HIGH_RATE_HZ = 100.0;
      constexpr double MEDIUM_RATE_HZ = 20.0;
      constexpr double DEFAULT_RATE_HZ = 5.0;

      // Calculate publisher index within the node
      int publisher_index = topic_id % num_topics;

      // Apply rate assignment logic based on publisher index and enabled flags
      if ((publisher_index == 0 || publisher_index == 1) && high_rate_enabled)
      {
        // First two publishers get high rate when enabled
        return HIGH_RATE_HZ;
      }
      else if ((publisher_index == 2 || publisher_index == 3) && medium_rate_enabled)
      {
        // Next two publishers get medium rate when enabled
        return MEDIUM_RATE_HZ;
      }
      else
      {
        // All others get default rate
        return DEFAULT_RATE_HZ;
      }
    }
    catch (const std::exception&)
    {
      // If parsing fails, return default rate
      return 5.0;
    }
  }

private:
  std::chrono::seconds window_size_;
  std::chrono::seconds update_interval_;
  std::unique_ptr<DynamicSubscriber> dynamic_subscriber_;

  std::mutex stats_mutex_;
  std::map<std::string, TopicStats> topic_stats_;

  std::thread stats_thread_;
  std::atomic<bool> stop_requested_{ false };
  std::chrono::steady_clock::time_point node_start_time_;
};
}  // namespace heph::ipc::zenoh::apps

auto main(int argc, const char* argv[]) -> int
{
  const heph::utils::StackTrace stack_trace;

  try
  {
    heph::telemetry::registerLogSink(std::make_unique<heph::telemetry::AbslLogSink>());

    auto desc = heph::cli::ProgramDescription("Monitor the frequency of messages on Zenoh topics.");
    heph::ipc::zenoh::appendProgramOption(desc);

    desc.defineOption<int>(
        "window-size",
        fmt::format("Time window in seconds for frequency calculation (Default: {}).", DEFAULT_WINDOW_SIZE_SECONDS),
        DEFAULT_WINDOW_SIZE_SECONDS);

    desc.defineOption<int>(
        "update-interval",
        fmt::format("Update interval in seconds for display refresh (Default: {}).", DEFAULT_UPDATE_INTERVAL_SECONDS),
        DEFAULT_UPDATE_INTERVAL_SECONDS);

    const auto args = std::move(desc).parse(argc, argv);

    auto [session_config, _, topic_filter_params] = heph::ipc::zenoh::parseProgramOptions(args);
    const auto window_size = std::chrono::seconds(args.getOption<int>("window-size"));
    const auto update_interval = std::chrono::seconds(args.getOption<int>("update-interval"));

    fmt::println("Opening session...");
    fmt::println("Monitoring all topics with {}s window, updating every {}s", window_size.count(),
                 update_interval.count());

    auto session = heph::ipc::zenoh::createSession(session_config);

    heph::ipc::zenoh::apps::TopicHz topic_hz{ std::move(session), topic_filter_params, window_size, update_interval };
    topic_hz.start().wait();

    heph::utils::TerminationBlocker::waitForInterrupt();

    topic_hz.stop().wait();

    return EXIT_SUCCESS;
  }
  catch (const std::exception& ex)
  {
    std::ignore = std::fputs(ex.what(), stderr);
    return EXIT_FAILURE;
  }
}
