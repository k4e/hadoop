package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class ContainerCRCheckpointEvent extends ContainerCREvent {

  private final Container container;
  private final String processId;
  private final ContainerMigrationProcessRequest request;
  
  public ContainerCRCheckpointEvent(Container container, String processId,
      ContainerMigrationProcessRequest request) {
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
  
  public ContainerMigrationProcessRequest getRequest() {
    return this.request;
  }
}
