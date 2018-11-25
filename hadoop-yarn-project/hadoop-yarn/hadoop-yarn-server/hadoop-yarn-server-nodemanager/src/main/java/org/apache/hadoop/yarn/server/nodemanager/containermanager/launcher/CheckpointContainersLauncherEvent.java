package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class CheckpointContainersLauncherEvent extends ContainersLauncherEvent
{

  private final String address;
  private final int port;
  
  public CheckpointContainersLauncherEvent(Container container, String address,
      int port) {
    super(container, ContainersLauncherEventType.CHECKPOINT_CONTAINER);
    this.address = address;
    this.port = port;
  }
  
  public String getAddress() {
    return address;
  }
  
  public int getPort() {
    return port;
  }
}
