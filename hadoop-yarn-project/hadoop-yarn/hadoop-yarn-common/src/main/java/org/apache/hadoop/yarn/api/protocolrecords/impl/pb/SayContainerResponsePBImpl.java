package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.SayContainerResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SayContainerResponseProto;

public class SayContainerResponsePBImpl extends SayContainerResponse {
  SayContainerResponseProto proto = SayContainerResponseProto.getDefaultInstance();
  SayContainerResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public SayContainerResponsePBImpl() {
    builder = SayContainerResponseProto.newBuilder();
  }
  
  public SayContainerResponsePBImpl(SayContainerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public SayContainerResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
}
