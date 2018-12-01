package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreRequest;
import org.apache.hadoop.yarn.api.records.NodeId;

public class RMNodeContainerRestoreEvent extends RMNodeEvent {

  private ContainerRestoreRequest request;
  
  public RMNodeContainerRestoreEvent(NodeId nodeId,
      ContainerRestoreRequest request) {
    super(nodeId, RMNodeEventType.RESTORE_CONTAINER);
    this.request = request;
  }
  
  public ContainerRestoreRequest getRestoreRequest() {
    return request;
  }
}
