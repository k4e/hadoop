package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class ContainerCRCheckpointEvent extends ContainerCREvent {

  private final Container container;
  private final String processId;
  
  public ContainerCRCheckpointEvent(Container container, String processId,
      ContainerMigrationProcessRequest request) {
    super(ContainerCREventType.CHECKPOINT, request);
    this.container = container;
    this.processId = processId;
  }
  
  public Container getContainer() {
    return this.container;
  }
  
  public String getProcessId() {
    return this.processId;
  }
}
