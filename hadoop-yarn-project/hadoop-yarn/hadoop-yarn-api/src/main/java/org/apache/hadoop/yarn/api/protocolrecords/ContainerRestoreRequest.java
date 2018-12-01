package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ContainerRestoreRequest {

  @Public
  @Unstable
  public static ContainerRestoreRequest newInstance(long id,
      ContainerId containerId, Token containerToken,
      ContainerId sourceContainerId, String address, int port) {
    ContainerRestoreRequest req =
        Records.newRecord(ContainerRestoreRequest.class);
    req.setContainerId(containerId);
    req.setContainerToken(containerToken);
    req.setSourceContainerId(sourceContainerId);
    req.setAddress(address);
    req.setPort(port);
    return req;
  }
  
  @Public
  @Unstable
  public abstract long getId();
  
  @Public
  @Unstable
  public abstract void setId(long id);
  
  @Public
  @Unstable
  public abstract ContainerId getContainerId();
  
  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  @Public
  @Unstable
  public abstract Token getContainerToken();
  
  @Public
  @Unstable
  public abstract void setContainerToken(Token containerToken);
  
  @Public
  @Unstable
  public abstract ContainerId getSourceContainerId();
  
  @Public
  @Unstable
  public abstract void setSourceContainerId(ContainerId sourceContainerId);
  
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
