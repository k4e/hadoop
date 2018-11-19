package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCheckpointResponseProto;

public class ContainerCheckpointResponsePBImpl
    extends ContainerCheckpointResponse {
  ContainerCheckpointResponseProto proto;
  ContainerCheckpointResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public ContainerCheckpointResponsePBImpl() {
    builder = ContainerCheckpointResponseProto.newBuilder();
  }
  
  public ContainerCheckpointResponsePBImpl(
      ContainerCheckpointResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerCheckpointResponseProto getProto() {
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
}
