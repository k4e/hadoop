package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class CheckpointContainersLauncherEvent extends ContainersLauncherEvent
{

  private final int port;
  
  public CheckpointContainersLauncherEvent(Container container, int port) {
    super(container, ContainersLauncherEventType.CHECKPOINT_CONTAINER);
    this.port = port;
  }
  
  public int getPort() {
    return port;
  }
}
