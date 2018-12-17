package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;

public class ContainerCROpenPageServerEvent extends ContainerCREvent {
  
  public ContainerCROpenPageServerEvent(ContainerMigrationProcessRequest request) {
    super(ContainerCREventType.OPEN_PAGE_SERVER, request);
  }
}
