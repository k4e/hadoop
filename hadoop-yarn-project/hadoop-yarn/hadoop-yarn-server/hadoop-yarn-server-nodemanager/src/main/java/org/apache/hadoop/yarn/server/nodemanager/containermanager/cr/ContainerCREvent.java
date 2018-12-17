package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;
import org.apache.hadoop.yarn.event.AbstractEvent;

public abstract class ContainerCREvent
    extends AbstractEvent<ContainerCREventType> {

  private final ContainerMigrationProcessRequest request;

  public ContainerCREvent(ContainerCREventType eventType, 
      ContainerMigrationProcessRequest request) {
    super(eventType, System.currentTimeMillis());
    this.request = request;
  }

  public ContainerMigrationProcessRequest getRequest() {
    return this.request;
  }
}
