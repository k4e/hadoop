package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class SayContainersLancherEvent extends ContainersLauncherEvent {

  private final String message;
  
  public SayContainersLancherEvent(Container container, String message) {
    super(container, ContainersLauncherEventType.SAY_CONTAINER);
    this.message = message;
  }
  
  public String getMessage() {
    return message;
  }
}
