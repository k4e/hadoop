package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ContainerCheckpointRequest {

  @Public
  @Unstable
  public static ContainerCheckpointRequest newInstance(
      ContainerId containerId, String address, int port) {
    ContainerCheckpointRequest req =
        Records.newRecord(ContainerCheckpointRequest.class);
    req.setContainerId(containerId);
    req.setAddress(address);
    req.setPort(port);
    return req;
  }
  
  @Public
  @Unstable
  public abstract ContainerId getContainerId();
  
  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);
  
  @Public
  @Unstable
  public abstract String getAddress();
  
  @Public
  @Unstable
  public abstract void setAddress(String address);
  
  @Public
  @Unstable
  public abstract int getPort();
  
  @Public
  @Unstable
  public abstract void setPort(int port);
}
