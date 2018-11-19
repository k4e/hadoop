package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.SayContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SayContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SayContainerRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class SayContainerRequestPBImpl extends SayContainerRequest {
  SayContainerRequestProto proto =
      SayContainerRequestProto.getDefaultInstance();
  SayContainerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId;
  private String message;
  
  public SayContainerRequestPBImpl() {
    this.builder = SayContainerRequestProto.newBuilder();
  }
  
  public SayContainerRequestPBImpl(SayContainerRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public SayContainerRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void mergeLocalToBuilder() {
    if (this.containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.message != null) {
      builder.setMessage(message);
    }
  }
  
  private void mergeLocalToProto() {
    if(viaProto) maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SayContainerRequestProto.newBuilder(proto);
    }
    viaProto = true;
  }
  
  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if(other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
  
  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
  
  @Override
  public ContainerId getContainerId() {
    SayContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }
  
  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if(containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  @Override
  public String getMessage() {
    SayContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.message != null) {
      return this.message;
    }
    if(!p.hasMessage()) {
      return null;
    }
    this.message = p.getMessage();
    return this.message;
  }

  @Override
  public void setMessage(String message) {
    maybeInitBuilder();
    if (message == null) {
      builder.clearMessage();
    }
    this.message = message;
  }
  
  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
}
