//=================================================================================================
// Copyright (C) 2023-2024 HEPHAESTUS Contributors
//=================================================================================================

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>

#include "hephaestus/cli/program_options.h"
#include "hephaestus/examples/types/pose.h"
#include "hephaestus/examples/types_proto/pose.h"  // NOLINT(misc-include-cleaner)
#include "hephaestus/examples/types/sample.h"
#include "hephaestus/examples/types_proto/sample.h"  // NOLINT(misc-include-cleaner)
#include "hephaestus/ipc/zenoh/program_options.h"
#include "hephaestus/ipc/zenoh/publisher.h"
#include "hephaestus/ipc/zenoh/session.h"
#include "hephaestus/ipc/zenoh/subscriber.h"
#include "hephaestus/telemetry/log.h"
#include "hephaestus/telemetry/log_sinks/absl_sink.h"
#include "hephaestus/utils/exception.h"
#include "hephaestus/utils/signal_handler.h"
#include "hephaestus/utils/stack_trace.h"
#include "zenoh_program_options.h"

const int HIGH_RATE_PUBLISH_RATE = 100;   // High publish rate in Hz
const int MEDIUM_RATE_PUBLISH_RATE = 20;  // Medium publish rate in Hz
const int DEFAULT_PUBLISH_RATE = 5;       // Default publish rate in Hz

auto main(int argc, const char* argv[]) -> int
{
  const heph::utils::StackTrace stack_trace;

  try
  {
    heph::telemetry::registerLogSink(std::make_unique<heph::telemetry::AbslLogSink>());

    // Force immediate flushing for Docker logging - applies globally
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // sleep for a short time to allow log sinks to initialize
    std::this_thread::sleep_for(std::chrono::milliseconds{ 1000 });

    auto desc = heph::cli::ProgramDescription("Zenoh pub/sub stress test example");
    desc.defineOption<int>("node-id", 'n', "Node ID for this instance");
    desc.defineOption<int>("num-topics", 'p', "Number of publishers per node", 3);
    desc.defineOption<std::string>("high-rate", 'H', "Enable high-rate publishing (100 Hz instead of 5 Hz)", "false");
    desc.defineOption<std::string>("medium-rate", 'M', "Enable medium-rate publishing (50 Hz instead of 5 Hz)",
                                   "false");
    desc.defineOption<int>("num-nodes", 'N', "Total number of nodes in the system", 5);
    desc.defineOption<int>("array-size", 'a', "Size of the data vector in StressMessage", 5);
    heph::ipc::zenoh::appendProgramOption(desc, getDefaultTopic(ExampleType::PUBSUB));
    const auto args = std::move(desc).parse(argc, argv);

    int node_id = args.getOption<int>("node-id");
    int num_topics = args.getOption<int>("num-topics");
    bool high_rate = (args.getOption<std::string>("high-rate") == "true");
    bool medium_rate = (args.getOption<std::string>("medium-rate") == "true");
    int num_nodes = args.getOption<int>("num-nodes");
    int array_size = args.getOption<int>("array-size");

    auto [session_config, topic_config, _] = heph::ipc::zenoh::parseProgramOptions(args);
    auto session = heph::ipc::zenoh::createSession(session_config);

    // Log the Zenoh session ID (ZID
    auto zenoh_id = session->zenoh_session.get_zid().to_string();
    fmt::println("Node {} started with Zenoh session ID: {}", node_id, zenoh_id);

    // Create publishers
    std::vector<std::unique_ptr<heph::ipc::zenoh::Publisher<heph::examples::types::StressMessage>>> publishers;
    std::vector<std::string> pub_topic_names;

    for (int j = 0; j < num_topics; ++j)
    {
      int pub_topic_id = node_id * num_topics + j;
      auto pub_topic_name = fmt::format("{}_{}", topic_config.name, pub_topic_id);
      pub_topic_names.push_back(pub_topic_name);

      auto pub_topic_config = topic_config;
      pub_topic_config.name = pub_topic_name;

      publishers.push_back(std::make_unique<heph::ipc::zenoh::Publisher<heph::examples::types::StressMessage>>(
          session, pub_topic_config, [j](const auto& status) {
            if (status.matching)
            {
              fmt::println("Publisher {} - Subscriber match", j + 1);
            }
            else
            {
              fmt::println("Publisher {} - NO subscriber matching", j + 1);
            }
          }));
    }

    fmt::println("Node {} declaring {} Publishers on topics: {}", node_id, num_topics,
                 fmt::join(pub_topic_names, ", "));

    // Create subscribers
    std::vector<std::unique_ptr<heph::ipc::zenoh::Subscriber<heph::examples::types::StressMessage>>> subscribers;
    std::vector<std::string> sub_topic_names;
    std::vector<std::size_t> sub_last_values(static_cast<std::size_t>(num_topics), 0);

    for (int j = 0; j < num_topics; ++j)
    {
      int topic_id = (node_id + j + 1) * num_topics % (num_nodes * num_topics) + j;
      auto sub_topic_name = fmt::format("{}_{}", topic_config.name, topic_id);
      sub_topic_names.push_back(sub_topic_name);

      auto sub_topic_config = topic_config;
      sub_topic_config.name = sub_topic_name;

      subscribers.push_back(std::make_unique<heph::ipc::zenoh::Subscriber<heph::examples::types::StressMessage>>(
          session, sub_topic_config,
          [node_id, sub_topic_name, &sub_last_values,
           j](const heph::ipc::zenoh::MessageMetadata& /**/,
              const std::shared_ptr<heph::examples::types::StressMessage>& msg) {
            auto idx = static_cast<std::size_t>(j);
            if (msg->counter != sub_last_values[idx] + 1 && sub_last_values[idx] != 0)
            {
              fmt::println(
                  "ERROR: Node {} - Subscriber {} received unexpected value on topic '{}': expected {}, got {}",
                  node_id, j + 1, sub_topic_name, sub_last_values[idx] + 1, msg->counter);
              std::cout.flush();
            }
            sub_last_values[idx] = msg->counter;
          }));
    }

    fmt::println("Node {} declaring {} Subscribers on topics: {}", node_id, num_topics,
                 fmt::join(sub_topic_names, ", "));

    fmt::println("Node {} starting to publish at {} Hz (high: {}, medium: {})", node_id,
                 high_rate ? HIGH_RATE_PUBLISH_RATE : (medium_rate ? MEDIUM_RATE_PUBLISH_RATE : DEFAULT_PUBLISH_RATE),
                 high_rate, medium_rate);

    // Create atomic stop flag for all publisher threads
    std::atomic<bool> stop_publishing{ false };

    // Create publisher threads
    std::vector<std::thread> publisher_threads;
    std::vector<std::atomic<std::size_t>> publisher_counters(static_cast<std::size_t>(num_topics));

    // Initialize counters
    for (std::size_t i = 0; i < static_cast<std::size_t>(num_topics); ++i)
    {
      std::size_t start_value = static_cast<std::size_t>(i * 100);
      // Stagger the initial values for each publisher
      publisher_counters[i] = start_value;
      fmt::println("Node {} - Publisher {} initialized with counter {}", node_id, i + 1, start_value);
      std::cout.flush();
    }

    // Calculate publishing intervals
    auto default_publish_interval = std::chrono::duration<double>(1.0 / DEFAULT_PUBLISH_RATE);
    auto high_rate_publish_interval = std::chrono::duration<double>(1.0 / HIGH_RATE_PUBLISH_RATE);
    auto medium_rate_publish_interval = std::chrono::duration<double>(1.0 / MEDIUM_RATE_PUBLISH_RATE);

    // Start a publisher thread for each publisher
    for (std::size_t i = 0; i < static_cast<std::size_t>(num_topics); ++i)
    {
      auto publish_interval = default_publish_interval;  // Default publish rate
      if (high_rate && (i == 0 || i == 1))
      {
        // Use high rate for first two publishers
        publish_interval = high_rate_publish_interval;
      }
      else if (medium_rate && (i == 2 || i == 3))
      {
        // Use medium rate for next two publishers
        publish_interval = medium_rate_publish_interval;
      }

      publisher_threads.emplace_back([&, i, publish_interval]() {
        auto& publisher = publishers[i];
        auto& counter = publisher_counters[i];

        heph::examples::types::StressMessage msg;
        // Resize data vector to a fixed size, e.g., 100 elements
        msg.data.resize(static_cast<std::size_t>(array_size), 0.0f);  // Fill with zeros

        auto next_publish_time = std::chrono::steady_clock::now();

        while (!stop_publishing && !heph::utils::TerminationBlocker::stopRequested())
        {
          counter++;
          msg.counter = counter.load();

          // Log progress for publisher 0 every 1000 messages
          if (counter % 1000 == 0)
          {
            // fmt::println("Node {} - Publisher {} published {} messages at {:.2f} Hz", node_id, i + 1, counter.load(),
            //              1.0 / publish_interval.count());
            std::cout.flush();
          }

          auto res = publisher->publish(msg);
          heph::panicIf(!res, "failed to publish message");

          // Sleep until next publish time
          next_publish_time += std::chrono::duration_cast<std::chrono::steady_clock::duration>(publish_interval);
          std::this_thread::sleep_until(next_publish_time);
        }

        fmt::println("Node {} - Publisher {} thread stopped after {} messages", node_id, i + 1, counter.load());
        std::cout.flush();
      });
    }

    // Wait for termination signal
    heph::utils::TerminationBlocker::waitForInterrupt();

    // Signal all threads to stop
    stop_publishing = true;

    // Wait for all publisher threads to finish
    for (auto& thread : publisher_threads)
    {
      if (thread.joinable())
      {
        thread.join();
      }
    }

    fmt::println("Node {} - All publisher threads stopped", node_id);

    return EXIT_SUCCESS;
  }
  catch (const std::exception& ex)
  {
    std::ignore = std::fputs(fmt::format("main terminated with an exception: {}\n", ex.what()).c_str(), stderr);
    return EXIT_FAILURE;
  }
}
