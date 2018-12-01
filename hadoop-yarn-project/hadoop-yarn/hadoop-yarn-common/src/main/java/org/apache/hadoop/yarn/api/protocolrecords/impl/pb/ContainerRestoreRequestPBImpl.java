package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerRestoreRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerRestoreRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ContainerRestoreRequestPBImpl extends ContainerRestoreRequest {

  ContainerRestoreRequestProto proto = ContainerRestoreRequestProto
      .getDefaultInstance();
  ContainerRestoreRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId;
  private Token containerToken;
  private ContainerId sourceContainerId;
  
  public ContainerRestoreRequestPBImpl() {
    this.builder = ContainerRestoreRequestProto.newBuilder();
  }
  
  public ContainerRestoreRequestPBImpl(ContainerRestoreRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerRestoreRequestProto getProto() {
    mergeLocalToProto();
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }
  
  @Override
  public long getId() {
    ContainerRestoreRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }
  
  @Override
  public void setId(long id) {
    maybeInitBuilder();
    this.builder.setId(id);
  }
  
  @Override
  public ContainerId getContainerId() {
    ContainerRestoreRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public Token getContainerToken() {
    ContainerRestoreRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerToken != null) {
      return this.containerToken;
    }
    if (!p.hasContainerToken()) {
      return null;
    }
    this.containerToken = convertFromProtoFormat(p.getContainerToken());
    return this.containerToken;
  }

  @Override
  public void setContainerToken(Token containerToken) {
    maybeInitBuilder();
    if(containerToken == null) {
      builder.clearContainerToken();
    }
    this.containerToken = containerToken;
  }

  @Override
  public ContainerId getSourceContainerId() {
    ContainerRestoreRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.sourceContainerId == null && p.hasSourceContainerId()) {
      this.sourceContainerId = convertFromProtoFormat(
          p.getSourceContainerId());
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
  public String getAddress() {
    ContainerRestoreRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getAddress();
  }

  @Override
  public void setAddress(String address) {
    maybeInitBuilder();
    this.builder.setAddress(address);
  }

  @Override
  public int getPort() {
    ContainerRestoreRequestProtoOrBuilder p = viaProto ? proto : builder;
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
    if (this.containerToken != null) {
      this.builder.setContainerToken(convertToProtoFormat(containerToken));
    }
    if (this.sourceContainerId != null) {
      this.builder.setSourceContainerId(convertToProtoFormat(
          sourceContainerId));
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
      this.builder = ContainerRestoreRequestProto.newBuilder(proto);
    }
    this.viaProto = false;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }
  
  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  
  private TokenPBImpl convertFromProtoFormat(TokenProto containerProto) {
    return new TokenPBImpl(containerProto);
  }

  private TokenProto convertToProtoFormat(Token container) {
    return ((TokenPBImpl)container).getProto();
  }
}
