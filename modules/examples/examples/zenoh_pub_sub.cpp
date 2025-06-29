//=================================================================================================
// Copyright (C) 2023-2024 HEPHAESTUS Contributors
//=================================================================================================

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

auto main(int argc, const char* argv[]) -> int
{
  const heph::utils::StackTrace stack_trace;

  try
  {
    heph::telemetry::registerLogSink(std::make_unique<heph::telemetry::AbslLogSink>());

    // sleep for a short time to allow log sinks to initialize
    std::this_thread::sleep_for(std::chrono::milliseconds{ 1000 });

    auto desc = heph::cli::ProgramDescription("Zenoh pub/sub stress test example");
    desc.defineOption<int>("node_id", 'n', "Node ID for this instance");
    heph::ipc::zenoh::appendProgramOption(desc, getDefaultTopic(ExampleType::PUBSUB));
    const auto args = std::move(desc).parse(argc, argv);

    int node_id = args.getOption<int>("node_id");

    auto [session_config, topic_config, _] = heph::ipc::zenoh::parseProgramOptions(args);
    auto session = heph::ipc::zenoh::createSession(session_config);

    const int NUM_NODES = 6;        // Total number of nodes in the system
    const int TOPICS_PER_NODE = 4;  // Each node publishes to x topics

    int pub_topic_id_1 = (node_id - 1) * TOPICS_PER_NODE + 1;
    int pub_topic_id_2 = (node_id - 1) * TOPICS_PER_NODE + 2;
    int pub_topic_id_3 = (node_id - 1) * TOPICS_PER_NODE + 3;
    int pub_topic_id_4 = (node_id - 1) * TOPICS_PER_NODE + 4;

    auto pub_topic_name_1 = fmt::format("{}_{}", topic_config.name, pub_topic_id_1);
    auto pub_topic_name_2 = fmt::format("{}_{}", topic_config.name, pub_topic_id_2);
    auto pub_topic_name_3 = fmt::format("{}_{}", topic_config.name, pub_topic_id_3);
    auto pub_topic_name_4 = fmt::format("{}_{}", topic_config.name, pub_topic_id_4);

    // Create topic configurations for each topic
    auto topic_config_1 = topic_config;
    topic_config_1.name = pub_topic_name_1;

    auto topic_config_2 = topic_config;
    topic_config_2.name = pub_topic_name_2;

    auto topic_config_3 = topic_config;
    topic_config_3.name = pub_topic_name_3;

    // Create 3 publishers, one for each topic
    heph::ipc::zenoh::Publisher<heph::examples::types::Pose> publisher_1{
      session, topic_config_1,
      [](const auto& status) {
        if (status.matching)
        {
          fmt::println("Publisher 1 - Subscriber match");
        }
        else
        {
          fmt::println("Publisher 1 - NO subscriber matching");
        }
      }
    };

    heph::ipc::zenoh::Publisher<heph::examples::types::Pose> publisher_2{
      session, topic_config_2,
      [](const auto& status) {
        if (status.matching)
        {
          fmt::println("Publisher 2 - Subscriber match");
        }
        else
        {
          fmt::println("Publisher 2 - NO subscriber matching");
        }
      }
    };

    heph::ipc::zenoh::Publisher<heph::examples::types::Pose> publisher_3{
      session, topic_config_3,
      [](const auto& status) {
        if (status.matching)
        {
          fmt::println("Publisher 3 - Subscriber match");
        }
        else
        {
          fmt::println("Publisher 3 - NO subscriber matching");
        }
      }
    };

    heph::ipc::zenoh::Publisher<heph::examples::types::Pose> publisher_4{
      session, topic_config,
      [](const auto& status) {
        if (status.matching)
        {
          fmt::println("Publisher 4 - Subscriber match");
        }
        else
        {
          fmt::println("Publisher 4 - NO subscriber matching");
        }
      }
    };

    fmt::println("Node {} declaring Publishers on topics: '{}', '{}', '{}', '{}'", node_id, pub_topic_name_1,
                 pub_topic_name_2, pub_topic_name_3, pub_topic_name_4);

    int topic_id_1 = (node_id + 1) * TOPICS_PER_NODE % (NUM_NODES * TOPICS_PER_NODE) + 1;
    int topic_id_2 = (node_id + 2) * TOPICS_PER_NODE % (NUM_NODES * TOPICS_PER_NODE) + 2;
    int topic_id_3 = (node_id + 3) * TOPICS_PER_NODE % (NUM_NODES * TOPICS_PER_NODE) + 3;
    int topic_id_4 = (node_id + 4) * TOPICS_PER_NODE % (NUM_NODES * TOPICS_PER_NODE) + 4;

    auto sub_topic_name_1 = fmt::format("{}_{}", topic_config.name, topic_id_1);
    auto sub_topic_name_2 = fmt::format("{}_{}", topic_config.name, topic_id_2);
    auto sub_topic_name_3 = fmt::format("{}_{}", topic_config.name, topic_id_3);
    auto sub_topic_name_4 = fmt::format("{}_{}", topic_config.name, topic_id_4);

    // Create topic configurations for each subscriber
    auto sub_topic_config_1 = topic_config;
    sub_topic_config_1.name = sub_topic_name_1;

    auto sub_topic_config_2 = topic_config;
    sub_topic_config_2.name = sub_topic_name_2;

    auto sub_topic_config_3 = topic_config;
    sub_topic_config_3.name = sub_topic_name_3;

    auto sub_topic_config_4 = topic_config;
    sub_topic_config_4.name = sub_topic_name_4;

    auto sub_1_last_value = 0.0;
    auto sub_2_last_value = 0.0;
    auto sub_3_last_value = 0.0;
    auto sub_4_last_value = 0.0;

    // Create 3 subscribers with unique callbacks
    heph::ipc::zenoh::Subscriber<heph::examples::types::Pose> subscriber_1{
      session, sub_topic_config_1,
      [node_id, sub_topic_name_1, &sub_1_last_value](const heph::ipc::zenoh::MessageMetadata& /**/,
                                                     const std::shared_ptr<heph::examples::types::Pose>& pose) {
        // fmt::println("Node {} - Subscriber 1 received on topic '{}': {}",
        //              node_id, sub_topic_name_1, *pose);
        if (pose->position.z() != sub_1_last_value + 1 && sub_1_last_value != 0)
        {
          fmt::println("ERROR: Node {} - Subscriber 1 received unexpected value on topic '{}': expected {}, got {}",
                       node_id, sub_topic_name_1, sub_1_last_value + 1, pose->position.z());
        }
        sub_1_last_value = pose->position.z();
      }
    };

    heph::ipc::zenoh::Subscriber<heph::examples::types::Pose> subscriber_2{
      session, sub_topic_config_2,
      [node_id, sub_topic_name_2, &sub_2_last_value](const heph::ipc::zenoh::MessageMetadata& /**/,
                                                     const std::shared_ptr<heph::examples::types::Pose>& pose) {
        // fmt::println("Node {} - Subscriber 2 received on topic '{}': {}",
        //              node_id, sub_topic_name_2, *pose);
        if (pose->position.z() != sub_2_last_value + 1 && sub_2_last_value != 0)
        {
          fmt::println("ERROR: Node {} - Subscriber 2 received unexpected value on topic '{}': expected {}, got {}",
                       node_id, sub_topic_name_2, sub_2_last_value + 1, pose->position.z());
        }
        sub_2_last_value = pose->position.z();
      }
    };

    heph::ipc::zenoh::Subscriber<heph::examples::types::Pose> subscriber_3{
      session, sub_topic_config_3,
      [node_id, sub_topic_name_3, &sub_3_last_value](const heph::ipc::zenoh::MessageMetadata& /**/,
                                                     const std::shared_ptr<heph::examples::types::Pose>& pose) {
        // fmt::println("Node {} - Subscriber 3 received on topic '{}': {}",
        //              node_id, sub_topic_name_3, *pose);
        if (pose->position.z() != sub_3_last_value + 1 && sub_3_last_value != 0)
        {
          fmt::println("ERROR: Node {} - Subscriber 3 received unexpected value on topic '{}': expected {}, got {}",
                       node_id, sub_topic_name_3, sub_3_last_value + 1, pose->position.z());
        }
        sub_3_last_value = pose->position.z();
      }
    };

    heph::ipc::zenoh::Subscriber<heph::examples::types::Pose> subscriber_4{
      session, sub_topic_config_4,
      [node_id, sub_topic_name_4, &sub_4_last_value](const heph::ipc::zenoh::MessageMetadata& /**/,
                                                     const std::shared_ptr<heph::examples::types::Pose>& pose) {
        // fmt::println("Node {} - Subscriber 4 received on topic '{}': {}",
        //              node_id, sub_topic_name_4, *pose);
        if (pose->position.z() != sub_4_last_value + 1 && sub_4_last_value != 0)
        {
          fmt::println("ERROR: Node {} - Subscriber 4 received unexpected value on topic '{}': expected {}, got {}",
                       node_id, sub_topic_name_4, sub_4_last_value + 1, pose->position.z());
        }
        sub_4_last_value = pose->position.z();
      }
    };

    fmt::println("Node {} declaring Subscribers on topics: '{}', '{}', '{}', '{}'", node_id, sub_topic_name_1,
                 sub_topic_name_2, sub_topic_name_3, sub_topic_name_4);

    // heph::telemetry::flushLogEntries();

    static constexpr auto LOOP_WAIT = std::chrono::milliseconds{ 20 };
    double count = 0;
    while (!heph::utils::TerminationBlocker::stopRequested())
    {
      heph::examples::types::Pose pose;
      pose.position = Eigen::Vector3d{ 1, 2, count++ };            // NOLINT
      pose.orientation = Eigen::Quaterniond{ 1., 0.1, 0.2, 0.3 };  // NOLINT

      if (static_cast<int>(count) % 100 == 0)
      {
        fmt::println("Node {} - Publishing Data ('{} : {})", node_id, pub_topic_name_1, pose);
      }

      // Publish to all 3 topics
      // fmt::println("Node {} - Publishing Data to topic '{}': {}", node_id, pub_topic_name_1, pose);
      auto res1 = publisher_1.publish(pose);
      heph::panicIf(!res1, "failed to publish message to topic 1");

      // fmt::println("Node {} - Publishing Data to topic '{}': {}", node_id, pub_topic_name_2, pose);
      auto res2 = publisher_2.publish(pose);
      heph::panicIf(!res2, "failed to publish message to topic 2");

      // fmt::println("Node {} - Publishing Data to topic '{}': {}", node_id, pub_topic_name_3, pose);
      auto res3 = publisher_3.publish(pose);
      heph::panicIf(!res3, "failed to publish message to topic 3");

      std::this_thread::sleep_for(LOOP_WAIT);
    }

    return EXIT_SUCCESS;
  }
  catch (const std::exception& ex)
  {
    std::ignore = std::fputs(fmt::format("main terminated with an exception: {}\n", ex.what()).c_str(), stderr);
    return EXIT_FAILURE;
  }
}
