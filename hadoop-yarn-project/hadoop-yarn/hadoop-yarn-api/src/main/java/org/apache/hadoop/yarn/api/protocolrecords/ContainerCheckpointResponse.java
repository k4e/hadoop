package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.classification.InterfaceAudience.Public;

@Public
@Evolving
public abstract class ContainerCheckpointResponse {
  
  public static final int SUCCESS = 0;
  public static final int FAILURE = -1;

  @Public
  @Unstable
  public static ContainerCheckpointResponse newInstance(long id, int status) {
    ContainerCheckpointResponse resp = Records.newRecord(
        ContainerCheckpointResponse.class);
    resp.setId(id);
    resp.setStatus(status);
    return resp;
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
  public abstract boolean hasDirectory();
  
  @Public
  @Unstable
  public abstract String getDirectory();
  
  @Public
  @Unstable
  public abstract void setDirectory(String directory);
}
