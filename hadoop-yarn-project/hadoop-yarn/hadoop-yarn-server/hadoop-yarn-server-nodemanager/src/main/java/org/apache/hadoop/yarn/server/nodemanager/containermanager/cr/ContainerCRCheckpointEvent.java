package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class ContainerCRCheckpointEvent extends ContainerCREvent {

  private final Container container;
  private final String processId;
  private final ContainerCheckpointRequest request;
  
  public ContainerCRCheckpointEvent(Container container, String processId,
      ContainerCheckpointRequest request) {
    super(ContainerCREventType.CHECKPOINT);
    this.container = container;
    this.processId = processId;
    this.request = request;
  }
  
  public Container getContainer() {
    return this.container;
  }
  
  public String getProcessId() {
    return this.processId;
  }
  
  public ContainerCheckpointRequest getRequest() {
    return this.request;
  }
}
