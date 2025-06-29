//=================================================================================================
// Copyright (C) 2023-2024 HEPHAESTUS Contributors
//=================================================================================================

#include "hephaestus/examples/types/sample.h"

#include "hephaestus/examples/types/proto/sample.pb.h"
#include "hephaestus/examples/types_proto/sample.h"

namespace heph::examples::types {

void toProto(proto::SampleRequest& proto_sample, const SampleRequest& sample) {
  proto_sample.set_initial_value(sample.initial_value);
  proto_sample.set_iterations_count(sample.iterations_count);
}

void fromProto(const proto::SampleRequest& proto_sample, SampleRequest& sample) {
  sample.initial_value = proto_sample.initial_value();
  sample.iterations_count = proto_sample.iterations_count();
}

void toProto(proto::SampleReply& proto_sample, const SampleReply& sample) {
  proto_sample.set_value(sample.value);
  proto_sample.set_counter(sample.counter);
}

void fromProto(const proto::SampleReply& proto_sample, SampleReply& sample) {
  sample.value = proto_sample.value();
  sample.counter = proto_sample.counter();
}

void toProto(proto::StressMessage& proto_sample, const StressMessage& sample) {
  proto_sample.set_counter(sample.counter);
  proto_sample.clear_data();
  for (const auto& value : sample.data) {
    proto_sample.add_data(value);
  }
}

void fromProto(const proto::StressMessage& proto_sample, StressMessage& sample) {
  sample.counter = proto_sample.counter();
  sample.data.clear();
  sample.data.reserve(static_cast<std::size_t>(proto_sample.data_size()));
  for (const auto& value : proto_sample.data()) {
    sample.data.push_back(value);
  }
}
}  // namespace heph::examples::types
