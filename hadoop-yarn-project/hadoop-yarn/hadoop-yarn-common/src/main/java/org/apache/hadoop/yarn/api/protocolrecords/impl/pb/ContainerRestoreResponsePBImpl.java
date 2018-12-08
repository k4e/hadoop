package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerRestoreResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerRestoreResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ContainerRestoreResponsePBImpl extends ContainerRestoreResponse {

  ContainerRestoreResponseProto proto = ContainerRestoreResponseProto
      .getDefaultInstance();
  ContainerRestoreResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public ContainerRestoreResponsePBImpl() {
    builder = ContainerRestoreResponseProto.newBuilder();
  }
  
  public ContainerRestoreResponsePBImpl(ContainerRestoreResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerRestoreResponseProto getProto() {
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
  
  @Override
  public long getId() {
    ContainerRestoreResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  @Override
  public void setId(long id) {
    maybeInitBuilder();
    this.builder.setId(id);
  }

  @Override
  public int getStatus() {
    ContainerRestoreResponseProtoOrBuilder p = viaProto ? proto : builder;
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
      this.builder = ContainerRestoreResponseProto.newBuilder(proto);
    }
    this.viaProto = false;
  }
}
