package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ContainerMigrationProcessRequest {

  public static ContainerMigrationProcessRequest newInstance(long id,
      ContainerMigrationProcessType type, ContainerId sourceContainerId,
      ContainerId destinationContainerId) {
    ContainerMigrationProcessRequest request = Records.newRecord(
        ContainerMigrationProcessRequest.class);
    request.setId(id);
    request.setType(type);
    request.setSourceContainerId(sourceContainerId);
    request.setDestinationContainerId(destinationContainerId);
    return request;
  }
  
  @Public
  @Unstable
  public abstract long getId();
  
  @Public
  @Unstable
  public abstract void setId(long id);
  
  @Public
  @Unstable
  public abstract void setType(ContainerMigrationProcessType type);
  
  @Public
  @Unstable
  public abstract ContainerMigrationProcessType getType();
  
  @Public
  @Unstable
  public abstract ContainerId getSourceContainerId();
  
  @Public
  @Unstable
  public abstract void setSourceContainerId(ContainerId sourceContainerId);
  
  @Public
  @Unstable
  public abstract ContainerId getDestinationContainerId();
  
  @Public
  @Unstable
  public abstract void setDestinationContainerId(
      ContainerId destinationContainerId);
  
  @Public
  @Unstable
  public abstract boolean hasDestinationAddress();
  
  @Public
  @Unstable
  public abstract String getDestinationAddress();
  
  @Public
  @Unstable
  public abstract void setDestinationAddress(String destinationAddress);
  
  @Public
  @Unstable
  public abstract boolean hasDestinationPort();
  
  @Public
  @Unstable
  public abstract int getDestinationPort();
  
  @Public
  @Unstable
  public abstract void setDestinationPort(int port);
  
  @Public
  @Unstable
  public abstract boolean hasImagesDir();
  
  @Public
  @Unstable
  public abstract String getImagesDir();
  
  @Public
  @Unstable
  public abstract void setImagesDir(String imagesDir);
}
