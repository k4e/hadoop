package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class SayContainerRequest {

  @Public
  @Unstable
  public static SayContainerRequest newInstance(ContainerId containerId,
      String message) {
    SayContainerRequest request =
        Records.newRecord(SayContainerRequest.class);
    request.setContainerId(containerId);
    request.setMessage(message);
    return request;
  }
  
  @Public
  @Unstable
  public abstract ContainerId getContainerId();
  
  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);
  
  @Public
  @Unstable
  public abstract String getMessage();
  
  @Public
  @Unstable
  public abstract void setMessage(String message);
}
