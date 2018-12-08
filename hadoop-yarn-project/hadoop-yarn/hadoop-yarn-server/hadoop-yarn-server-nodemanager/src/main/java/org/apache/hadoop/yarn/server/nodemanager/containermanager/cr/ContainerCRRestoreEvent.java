package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreRequest;

public class ContainerCRRestoreEvent extends ContainerCREvent {

  private final ContainerRestoreRequest request;
  
  public ContainerCRRestoreEvent(ContainerRestoreRequest request) {
    super(ContainerCREventType.RESTORE);
    this.request = request;
  }
  
  public ContainerRestoreRequest getRestoreRequest() {
    return this.request;
  }
}
