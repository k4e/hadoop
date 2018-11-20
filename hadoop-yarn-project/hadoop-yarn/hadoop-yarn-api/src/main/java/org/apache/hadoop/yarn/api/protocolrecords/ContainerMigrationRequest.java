package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class ContainerMigrationRequest {

  @Public
  @Unstable
  public static ContainerMigrationRequest newInstance(ContainerId containerId,
      NodeId destination) {
    ContainerMigrationRequest req =
        Records.newRecord(ContainerMigrationRequest.class);
    req.setContainerId(containerId);
    req.setDestination(destination);
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
  public abstract NodeId getDestination();
  
  @Public
  @Unstable
  public abstract void setDestination(NodeId destination);
}
