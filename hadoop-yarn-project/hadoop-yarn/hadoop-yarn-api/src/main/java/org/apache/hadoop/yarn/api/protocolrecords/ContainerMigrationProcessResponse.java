package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ContainerMigrationProcessResponse {
  
  public static final int SUCCESS = 0;
  public static final int FAILURE = -1;
  
  public static ContainerMigrationProcessResponse newInstance(long id,
      int status) {
    ContainerMigrationProcessResponse response = Records.newRecord(
        ContainerMigrationProcessResponse.class);
    response.setId(id);
    response.setStatus(status);
    return response;
  }
  
  @Public
  @Unstable
  public abstract long getId();
  
  @Public
  @Unstable
  public abstract void setId(long id);
  
  @Public
  @Unstable
  public abstract int getStatus();
  
  @Public
  @Unstable
  public abstract void setStatus(int status);
  
  @Public
  @Unstable
  public abstract boolean hasImagesDir();
  
  @Public
  @Unstable
  public abstract String getImagesDir();
  
  @Public
  @Unstable
  public abstract void setImagesDir(String imagesDir);
  
  @Public
  @Unstable
  public abstract ContainerLaunchContext getContainerLaunchContext();
  
  @Public
  @Unstable
  public abstract void setContainerLaunchContext(
      ContainerLaunchContext containerLaunchContext);
}
