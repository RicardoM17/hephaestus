//=================================================================================================
// Copyright (C) 2023-2024 HEPHAESTUS Contributors
//=================================================================================================

#pragma once

#include "hephaestus/examples/types/proto/sample.pb.h"
#include "hephaestus/examples/types/sample.h"
#include "hephaestus/serdes/protobuf/concepts.h"

namespace heph::serdes::protobuf {
template <>
struct ProtoAssociation<heph::examples::types::SampleRequest> {
  using Type = heph::examples::types::proto::SampleRequest;
};

template <>
struct ProtoAssociation<heph::examples::types::SampleReply> {
  using Type = heph::examples::types::proto::SampleReply;
};

template <>
struct ProtoAssociation<heph::examples::types::StressMessage> {
  using Type = heph::examples::types::proto::StressMessage;
};
}  // namespace heph::serdes::protobuf

namespace heph::examples::types {
void toProto(proto::SampleRequest& proto_sample, const SampleRequest& sample);
void fromProto(const proto::SampleRequest& proto_sample, SampleRequest& sample);

void toProto(proto::SampleReply& proto_sample, const SampleReply& sample);
void fromProto(const proto::SampleReply& proto_sample, SampleReply& sample);

void toProto(proto::StressMessage& proto_sample, const StressMessage& sample);
void fromProto(const proto::StressMessage& proto_sample, StressMessage& sample);

}  // namespace heph::examples::types
