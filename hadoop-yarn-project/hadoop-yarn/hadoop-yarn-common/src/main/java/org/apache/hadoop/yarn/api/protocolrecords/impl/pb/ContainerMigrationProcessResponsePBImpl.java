package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessResponse;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerMigrationProcessResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerMigrationProcessResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ContainerMigrationProcessResponsePBImpl extends ContainerMigrationProcessResponse {

  ContainerMigrationProcessResponseProto proto =
      ContainerMigrationProcessResponseProto.getDefaultInstance();
  ContainerMigrationProcessResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerLaunchContext containerLaunchContext = null;
  
  public ContainerMigrationProcessResponsePBImpl() {
    this.builder = ContainerMigrationProcessResponseProto.newBuilder();
  }
  
  public ContainerMigrationProcessResponsePBImpl(ContainerMigrationProcessResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
  
  public ContainerMigrationProcessResponseProto getProto() {
    this.proto = viaProto ? proto : builder.build();
    this.viaProto = true;
    return proto;
  }

  @Override
  public long getId() {
    ContainerMigrationProcessResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  @Override
  public void setId(long id) {
    maybeInitBuilder();
    this.builder.setId(id);
  }

  @Override
  public int getStatus() {
    ContainerMigrationProcessResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStatus();
  }

  @Override
  public void setStatus(int status) {
    maybeInitBuilder();
    this.builder.setStatus(status);
  }

  @Override
  public boolean hasImagesDir() {
    ContainerMigrationProcessResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasImagesDir();
  }

  @Override
  public String getImagesDir() {
    ContainerMigrationProcessResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getImagesDir();
  }

  @Override
  public void setImagesDir(String imagesDir) {
    maybeInitBuilder();
    this.builder.setImagesDir(imagesDir);
  }

  @Override
  public ContainerLaunchContext getContainerLaunchContext() {
    ContainerMigrationProcessResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerLaunchContext != null) {
      return this.containerLaunchContext;
    }
    if (!p.hasContainerLaunchContext()) {
      return null;
    }
    this.containerLaunchContext = convertFromProtoFormat(p.getContainerLaunchContext());
    return this.containerLaunchContext;
  }

  @Override
  public void setContainerLaunchContext(ContainerLaunchContext containerLaunchContext) {
    maybeInitBuilder();
    if (containerLaunchContext == null) {
      builder.clearContainerLaunchContext();
    }
    this.containerLaunchContext = containerLaunchContext;
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
    if (this.containerLaunchContext != null) {
      builder.setContainerLaunchContext(convertToProtoFormat(this.containerLaunchContext));
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
      this.builder = ContainerMigrationProcessResponseProto.newBuilder(proto);
    }
    this.viaProto = false;
  }
  
  
  private ContainerLaunchContextPBImpl convertFromProtoFormat(ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }

  private ContainerLaunchContextProto convertToProtoFormat(ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl)t).getProto();
  }
}
