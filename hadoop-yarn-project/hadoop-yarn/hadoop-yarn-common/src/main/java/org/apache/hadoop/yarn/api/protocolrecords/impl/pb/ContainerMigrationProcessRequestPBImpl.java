package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerMigrationProcessRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerMigrationProcessRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ContainerMigrationProcessRequestPBImpl extends ContainerMigrationProcessRequest {
  
  private ContainerMigrationProcessRequestProto proto =
      ContainerMigrationProcessRequestProto.getDefaultInstance();
  private ContainerMigrationProcessRequestProto.Builder builder = null;
  private boolean viaProto = false;
  
  private ContainerId sourceContainerId;
  private ContainerId destinationContainerId;
  
  public ContainerMigrationProcessRequestPBImpl() {
    this.builder = ContainerMigrationProcessRequestProto.newBuilder();
  }
  
  public ContainerMigrationProcessRequestPBImpl(ContainerMigrationProcessRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerMigrationProcessRequestProto getProto() {
    mergeLocalToProto();
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
  
  @Override
  public long getId() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  @Override
  public void setId(long id) {
    maybeInitBuilder();
    this.builder.setId(id);
  }

  @Override
  public ContainerMigrationProcessType getType() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasType()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getType());
  }
  
  @Override
  public void setType(ContainerMigrationProcessType type) {
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
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
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
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public boolean hasDestinationAddress() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasDestinationAddress();
  }
  
  @Override
  public String getDestinationAddress() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDestinationAddress();
  }
  
  @Override
  public void setDestinationAddress(String destinationAddress) {
    maybeInitBuilder();
    this.builder.setDestinationAddress(destinationAddress);
  }
  
  @Override
  public boolean hasDestinationPort() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasDestinationPort();
  }
  
  @Override
  public int getDestinationPort() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDestinationPort();
  }
  
  @Override
  public void setDestinationPort(int destinationPort) {
    maybeInitBuilder();
    this.builder.setDestinationPort(destinationPort);
  }
  
  @Override
  public boolean hasImagesDir() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasImagesDir();
  }
  
  @Override
  public String getImagesDir() {
    ContainerMigrationProcessRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getImagesDir();
  }
  
  @Override
  public void setImagesDir(String imagesDir) {
    maybeInitBuilder();
    this.builder.setImagesDir(imagesDir);
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
      this.builder = ContainerMigrationProcessRequestProto.newBuilder(proto);
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
