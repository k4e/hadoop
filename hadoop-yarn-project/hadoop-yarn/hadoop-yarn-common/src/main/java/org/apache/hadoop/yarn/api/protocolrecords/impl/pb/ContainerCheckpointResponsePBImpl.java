package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCheckpointResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCheckpointResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

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
  
  @Override
  public long getId() {
    ContainerCheckpointResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }
  
  @Override
  public void setId(long id) {
    maybeInitBuilder();
    this.builder.setId(id);
  }

  @Override
  public int getStatus() {
    ContainerCheckpointResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStatus();
  }

  @Override
  public void setStatus(int status) {
    maybeInitBuilder();
    this.builder.setStatus(status);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      this.builder = ContainerCheckpointResponseProto.newBuilder(proto);
    }
    this.viaProto = false;
  }
}
