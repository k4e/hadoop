package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerMigrationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerMigrationRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ContainerMigrationRequestPBImpl
    extends ContainerMigrationRequest {
  ContainerMigrationRequestProto proto =
      ContainerMigrationRequestProto.getDefaultInstance();
  ContainerMigrationRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId;
  private NodeId destination;
  
  public ContainerMigrationRequestPBImpl() {
    this.builder = ContainerMigrationRequestProto.newBuilder();
  }
  
  public ContainerMigrationRequestPBImpl(ContainerMigrationRequestProto proto)
  {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerMigrationRequestProto getProto() {
    mergeLocalToProto();
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
  
  @Override
  public ContainerId getContainerId() {
    ContainerMigrationRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public NodeId getDestination() {
    ContainerMigrationRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.destination == null && p.hasDestination()) {
      this.destination = convertFromProtoFormat(p.getDestination());
    }
    return this.destination;
  }

  @Override
  public void setDestination(NodeId destination) {
    maybeInitBuilder();
    if (destination == null) {
      builder.clearDestination();
    }
    this.destination = destination;
  }
  
  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
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
      this.builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.destination != null) {
      this.builder.setDestination(convertToProtoFormat(this.destination));
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
      this.builder = ContainerMigrationRequestProto.newBuilder(proto);
    }
    this.viaProto = false;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }
  
  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  
  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }
  
  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl)t).getProto();
  }
}
