package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreRequest;

public class CMgrContainerRestoreEvent extends ContainerManagerEvent {

  private List<ContainerRestoreRequest> containerRestores;
  
  public CMgrContainerRestoreEvent(
      List<ContainerRestoreRequest> containerRestores) {
    super(ContainerManagerEventType.RESTORE_CONTAINERS);
    this.containerRestores = containerRestores;
  }
  
  public List<ContainerRestoreRequest> getContainerRestores() {
    return this.containerRestores;
  }
}
