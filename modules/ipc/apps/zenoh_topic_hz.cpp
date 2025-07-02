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

namespace {
constexpr auto DEFAULT_WINDOW_SIZE_SECONDS = 10;
constexpr auto DEFAULT_UPDATE_INTERVAL_SECONDS = 1;

struct TopicStats {
  std::vector<std::chrono::steady_clock::time_point> message_times;
  std::size_t total_messages = 0;
  std::chrono::steady_clock::time_point first_message_time;
  std::chrono::steady_clock::time_point last_message_time;
};
}  // namespace

namespace heph::ipc::zenoh::apps {
class TopicHz {
public:
  TopicHz(SessionPtr session, TopicFilterParams topic_filter_params, std::chrono::seconds window_size,
          std::chrono::seconds update_interval)
    : window_size_(window_size), update_interval_(update_interval) {
    
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

  [[nodiscard]] auto start() -> std::future<void> {
    auto future = dynamic_subscriber_->start();
    
    // Start the stats reporting thread
    stats_thread_ = std::thread([this]() {
      while (!stop_requested_) {
        std::this_thread::sleep_for(update_interval_);
        if (!stop_requested_) {
          printStats();
        }
      }
    });
    
    return future;
  }

  [[nodiscard]] auto stop() -> std::future<void> {
    stop_requested_ = true;
    if (stats_thread_.joinable()) {
      stats_thread_.join();
    }
    return dynamic_subscriber_->stop();
  }

private:
  void initTopicStats(const std::string& topic) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    if (topic_stats_.find(topic) == topic_stats_.end()) {
      topic_stats_[topic] = TopicStats{};
      fmt::println("Monitoring topic: {}", topic);
    }
  }

  void recordMessage(const std::string& topic) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto now = std::chrono::steady_clock::now();
    
    auto& stats = topic_stats_[topic];
    stats.total_messages++;
    stats.last_message_time = now;
    
    if (stats.total_messages == 1) {
      stats.first_message_time = now;
    }
    
    // Add message time to rolling window
    stats.message_times.push_back(now);
    
    // Remove old messages outside the window
    auto cutoff_time = now - window_size_;
    stats.message_times.erase(
        std::remove_if(stats.message_times.begin(), stats.message_times.end(),
                      [cutoff_time](const auto& time) { return time < cutoff_time; }),
        stats.message_times.end());
  }

  void printStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto now = std::chrono::steady_clock::now();
    
    // Clear screen for live updates
    fmt::print("\033[2J\033[H");
    
    fmt::println("=== Topic Frequency Statistics (Window: {}s) ===", window_size_.count());
    fmt::println("Timestamp: {}", std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count());
    fmt::println("");
    
    if (topic_stats_.empty()) {
      fmt::println("No topics discovered yet...");
      return;
    }
    
    fmt::println("{:<40} {:<15} {:<15} {:<15}", "Topic", "Freq (Hz)", "Total Msgs", "Avg Freq (Hz)");
    fmt::println("{:-<85}", "");
    
    for (const auto& [topic, stats] : topic_stats_) {
      double current_freq = 0.0;
      double avg_freq = 0.0;
      
      // Calculate frequency in current window
      if (stats.message_times.size() > 1) {
        auto window_duration = std::chrono::duration_cast<std::chrono::duration<double>>(
            std::min(window_size_, std::chrono::duration_cast<std::chrono::seconds>(now - stats.first_message_time)));
        if (window_duration.count() > 0) {
          current_freq = static_cast<double>(stats.message_times.size()) / window_duration.count();
        }
      }
      
      // Calculate average frequency since first message
      if (stats.total_messages > 1) {
        auto total_duration = std::chrono::duration_cast<std::chrono::duration<double>>(
            stats.last_message_time - stats.first_message_time);
        if (total_duration.count() > 0) {
          avg_freq = static_cast<double>(stats.total_messages - 1) / total_duration.count();
        }
      }
      
      fmt::println("{:<40} {:<15.2f} {:<15} {:<15.2f}", 
                   topic, current_freq, stats.total_messages, avg_freq);
    }
    
    fmt::println("");
    fmt::println("Press Ctrl+C to stop monitoring...");
  }

private:
  std::chrono::seconds window_size_;
  std::chrono::seconds update_interval_;
  std::unique_ptr<DynamicSubscriber> dynamic_subscriber_;
  
  std::mutex stats_mutex_;
  std::map<std::string, TopicStats> topic_stats_;
  
  std::thread stats_thread_;
  std::atomic<bool> stop_requested_{false};
};
}  // namespace heph::ipc::zenoh::apps

auto main(int argc, const char* argv[]) -> int {
  const heph::utils::StackTrace stack_trace;

  try {
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
    fmt::println("Monitoring all topics with {}s window, updating every {}s", 
                 window_size.count(), update_interval.count());

    auto session = heph::ipc::zenoh::createSession(session_config);

    heph::ipc::zenoh::apps::TopicHz topic_hz{ std::move(session), topic_filter_params, window_size, update_interval };
    topic_hz.start().wait();

    heph::utils::TerminationBlocker::waitForInterrupt();

    topic_hz.stop().wait();

    return EXIT_SUCCESS;
  } catch (const std::exception& ex) {
    std::ignore = std::fputs(ex.what(), stderr);
    return EXIT_FAILURE;
  }
}
