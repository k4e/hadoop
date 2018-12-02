package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCRFinishRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCRType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCRFinishRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerCRFinishRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ContainerCRFinishRequestPBImpl extends ContainerCRFinishRequest {
  
  private ContainerCRFinishRequestProto proto =
      ContainerCRFinishRequestProto.getDefaultInstance();
  private ContainerCRFinishRequestProto.Builder builder = null;
  private boolean viaProto = false;
  
  private ContainerId sourceContainerId;
  private ContainerId destinationContainerId;
  
  public ContainerCRFinishRequestPBImpl() {
    this.builder = ContainerCRFinishRequestProto.newBuilder();
  }
  
  public ContainerCRFinishRequestPBImpl(ContainerCRFinishRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerCRFinishRequestProto getProto() {
    mergeLocalToProto();
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
  
  @Override
  public long getId() {
    ContainerCRFinishRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  @Override
  public void setId(long id) {
    maybeInitBuilder();
    this.builder.setId(id);
  }

  @Override
  public ContainerCRType getType() {
    ContainerCRFinishRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasType()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getType());
  }
  
  @Override
  public void setType(ContainerCRType type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearType();
    }
    else {
      builder.setType(ProtoUtils.convertToProtoFormat(type));
    }
  }

  @Override
  public ContainerId getSourceContainerId() {
    ContainerCRFinishRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.sourceContainerId == null && p.hasSourceContainerId()) {
      this.sourceContainerId = convertFromProtoFormat(p.getSourceContainerId());
    }
    return this.sourceContainerId;
  }

  @Override
  public void setSourceContainerId(ContainerId sourceContainerId) {
    maybeInitBuilder();
    if (sourceContainerId == null) {
      builder.clearSourceContainerId();
    }
    this.sourceContainerId = sourceContainerId;
  }

  @Override
  public ContainerId getDestinationContainerId() {
    ContainerCRFinishRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.destinationContainerId == null && p.hasDestinationContainerId()) {
      this.destinationContainerId = convertFromProtoFormat(
          p.getDestinationContainerId());
    }
    return this.destinationContainerId;
  }

  @Override
  public void setDestinationContainerId(ContainerId destinationContainerId) {
    maybeInitBuilder();
    if (destinationContainerId == null) {
      builder.clearDestinationContainerId();
    }
    this.destinationContainerId = destinationContainerId;
  }

  @Override
  public boolean getCompleting() {
    ContainerCRFinishRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCompleting();
  }

  @Override
  public void setCompleting(boolean completing) {
    maybeInitBuilder();
    this.builder.setCompleting(completing);
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
    if (this.sourceContainerId != null) {
      this.builder.setSourceContainerId(
          convertToProtoFormat(sourceContainerId));
    }
    if (this.destinationContainerId != null) {
      this.builder.setDestinationContainerId(
          convertToProtoFormat(destinationContainerId));
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
      this.builder = ContainerCRFinishRequestProto.newBuilder(proto);
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
