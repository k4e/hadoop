package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;

public class ContainerCROpenReceiver extends ContainerCREvent {
  
  public ContainerCROpenReceiver(ContainerMigrationProcessRequest request) {
    super(ContainerCREventType.OPEN_RECEIVER, request);
  }
}
