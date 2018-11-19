package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerMigrationResponseProto;

public class ContainerMigrationResponsePBImpl
    extends ContainerMigrationResponse {
  ContainerMigrationResponseProto proto =
      ContainerMigrationResponseProto.getDefaultInstance();
  ContainerMigrationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public ContainerMigrationResponsePBImpl() {
    this.builder = ContainerMigrationResponseProto.newBuilder();
  }
  
  public ContainerMigrationResponsePBImpl(
      ContainerMigrationResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerMigrationResponseProto getProto() {
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
}
