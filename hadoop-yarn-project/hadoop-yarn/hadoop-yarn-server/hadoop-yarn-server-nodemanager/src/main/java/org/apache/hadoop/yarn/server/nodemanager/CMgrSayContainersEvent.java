package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.SayContainerRequest;

public class CMgrSayContainersEvent extends ContainerManagerEvent {

  private List<SayContainerRequest> sayContainers;
  
  public CMgrSayContainersEvent(List<SayContainerRequest> sayContainers) {
    super(ContainerManagerEventType.SAY_CONTAINERS);
    this.sayContainers = sayContainers;
  }
  
  public List<SayContainerRequest> getSayContainers() {
    return this.sayContainers;
  }
}
