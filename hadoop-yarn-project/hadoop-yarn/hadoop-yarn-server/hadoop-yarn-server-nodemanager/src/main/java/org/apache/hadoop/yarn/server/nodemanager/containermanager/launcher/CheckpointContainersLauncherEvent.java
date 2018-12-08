package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class CheckpointContainersLauncherEvent extends ContainersLauncherEvent
{

  private final ContainerCheckpointRequest request;
  
  public CheckpointContainersLauncherEvent(Container container,
      ContainerCheckpointRequest request) {
    super(container, ContainersLauncherEventType.CHECKPOINT_CONTAINER);
    this.request = request;
  }
  
  public ContainerCheckpointRequest getCheckpointRequest() {
    return request;
  }
}
