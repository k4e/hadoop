package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;
import org.apache.hadoop.yarn.api.records.NodeId;

public class RMNodeContainerCheckpointEvent extends RMNodeEvent {

  private ContainerCheckpointRequest request;
  
  public RMNodeContainerCheckpointEvent(NodeId nodeId,
      ContainerCheckpointRequest request) {
    super(nodeId, RMNodeEventType.CHECKPOINT_CONTAINER);
    this.request = request;
  }
  
  public ContainerCheckpointRequest getCheckpointRequest() {
    return request;
  }
}
