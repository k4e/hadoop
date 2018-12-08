package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.event.AbstractEvent;

public abstract class ContainerCREvent
    extends AbstractEvent<ContainerCREventType> {

  public ContainerCREvent(ContainerCREventType eventType) {
    super(eventType, System.currentTimeMillis());
  }
}
