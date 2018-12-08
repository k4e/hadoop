package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ContainerCRFinishRequest {

  public static ContainerCRFinishRequest newInstance(long id,
      ContainerCRType type, ContainerId sourceContainerId,
      ContainerId destinationContainerId, boolean completing) {
    ContainerCRFinishRequest req = Records.newRecord(
        ContainerCRFinishRequest.class);
    req.setId(id);
    req.setType(type);
    req.setSourceContainerId(sourceContainerId);
    req.setDestinationContainerId(destinationContainerId);
    req.setCompleting(completing);
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
  public abstract void setType(ContainerCRType type);
  
  @Public
  @Unstable
  public abstract ContainerCRType getType();
  
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
  public abstract boolean getCompleting();
  
  @Public
  @Unstable
  public abstract void setCompleting(boolean completing);
}
