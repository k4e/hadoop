package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class CheckpointContainersLauncherEvent extends ContainersLauncherEvent {

  private final ContainerMigrationProcessRequest request;
  
  public CheckpointContainersLauncherEvent(Container container,
      ContainerMigrationProcessRequest request) {
    super(container, ContainersLauncherEventType.CHECKPOINT_CONTAINER);
    this.request = request;
  }
  
  public ContainerMigrationProcessRequest getRequest() {
    return request;
  }
}
