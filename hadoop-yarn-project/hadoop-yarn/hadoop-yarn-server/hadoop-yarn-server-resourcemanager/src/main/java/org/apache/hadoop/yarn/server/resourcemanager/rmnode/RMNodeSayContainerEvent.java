package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.protocolrecords.SayContainerRequest;
import org.apache.hadoop.yarn.api.records.NodeId;

public class RMNodeSayContainerEvent extends RMNodeEvent {

  private SayContainerRequest sayRequest;
  
  public RMNodeSayContainerEvent(NodeId nodeId,
      SayContainerRequest sayRequest) {
    super(nodeId, RMNodeEventType.SAY_CONTAINER);
    this.sayRequest = sayRequest;
  }
  
  public SayContainerRequest getSayRequest() {
    return this.sayRequest;
  }
}
