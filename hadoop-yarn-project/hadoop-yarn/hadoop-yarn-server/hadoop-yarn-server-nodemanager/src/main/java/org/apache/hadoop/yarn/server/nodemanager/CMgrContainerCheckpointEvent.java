package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;

public class CMgrContainerCheckpointEvent extends ContainerManagerEvent {

  private List<ContainerCheckpointRequest> containerCheckpoints;
  
  public CMgrContainerCheckpointEvent(
      List<ContainerCheckpointRequest> containerCheckpoints) {
    super(ContainerManagerEventType.CHECKPOINT_CONTAINERS);
    this.containerCheckpoints = containerCheckpoints;
  }
  
  public List<ContainerCheckpointRequest> getContainerCheckpoints() {
    return containerCheckpoints;
  }
}
