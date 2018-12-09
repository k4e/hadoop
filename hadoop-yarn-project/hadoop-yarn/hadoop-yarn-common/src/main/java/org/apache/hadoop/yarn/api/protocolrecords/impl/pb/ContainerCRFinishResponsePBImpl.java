package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCRFinishResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCRFinishResponseProto;

public class ContainerCRFinishResponsePBImpl extends ContainerCRFinishResponse {

  ContainerCRFinishResponseProto proto =
      ContainerCRFinishResponseProto.getDefaultInstance();
  ContainerCRFinishResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public ContainerCRFinishResponsePBImpl() {
    this.builder = ContainerCRFinishResponseProto.newBuilder();
  }
  
  public ContainerCRFinishResponsePBImpl(ContainerCRFinishResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerCRFinishResponseProto getProto() {
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
}
