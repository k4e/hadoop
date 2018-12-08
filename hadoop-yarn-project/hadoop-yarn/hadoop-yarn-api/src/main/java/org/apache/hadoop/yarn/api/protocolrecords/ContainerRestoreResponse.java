package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ContainerRestoreResponse {
  
  public static final int SUCCESS = 0;
  public static final int FAILURE = -1;
  
  @Public
  @Unstable
  public static ContainerRestoreResponse newInstance(long id, int status) {
    ContainerRestoreResponse resp = Records.newRecord(
        ContainerRestoreResponse.class);
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
}
