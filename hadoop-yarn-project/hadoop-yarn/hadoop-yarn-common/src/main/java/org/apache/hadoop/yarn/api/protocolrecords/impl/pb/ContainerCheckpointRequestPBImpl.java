package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCheckpointRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCheckpointRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ContainerCheckpointRequestPBImpl
    extends ContainerCheckpointRequest {
  ContainerCheckpointRequestProto proto =
      ContainerCheckpointRequestProto.getDefaultInstance();
  ContainerCheckpointRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId;
  
  public ContainerCheckpointRequestPBImpl() {
    this.builder = ContainerCheckpointRequestProto.newBuilder();
  }
  
  public ContainerCheckpointRequestPBImpl(
      ContainerCheckpointRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerCheckpointRequestProto getProto() {
    mergeLocalToProto();
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
  
  @Override
  public ContainerId getContainerId() {
    ContainerCheckpointRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId == null && p.hasContainerId()) {
      this.containerId = convertFromProtoFormat(p.getContainerId());
    }
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  @Override
  public int getPort() {
    ContainerCheckpointRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getPort();
  }

  @Override
  public void setPort(int port) {
    maybeInitBuilder();
    this.builder.setPort(port);
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
  
  private void mergeLocalToBuilder() {
    if (this.containerId != null) {
      this.builder.setContainerId(convertToProtoFormat(containerId));
    }
  }
  
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    this.proto = builder.build();
    this.viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      this.builder = ContainerCheckpointRequestProto.newBuilder(proto);
    }
    this.viaProto = false;
  }
  
  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }
  
  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
}
