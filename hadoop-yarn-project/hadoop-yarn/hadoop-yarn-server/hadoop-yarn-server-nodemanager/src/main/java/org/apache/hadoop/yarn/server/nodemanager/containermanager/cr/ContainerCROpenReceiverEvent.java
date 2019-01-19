package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;

public class ContainerCROpenReceiverEvent extends ContainerCREvent {
  
  public ContainerCROpenReceiverEvent(ContainerMigrationProcessRequest request) {
    super(ContainerCREventType.OPEN_RECEIVER, request);
  }
}
