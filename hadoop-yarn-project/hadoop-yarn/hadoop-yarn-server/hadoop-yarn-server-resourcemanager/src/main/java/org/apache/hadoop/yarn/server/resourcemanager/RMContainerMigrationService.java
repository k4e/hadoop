package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeContainerCheckpointEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;

public class RMContainerMigrationService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(RMContainerMigrationService.class);
  private static final int PORT = 65432;
  // TODO 確実にこのサービスがアロケートしたコンテナであることがわかるような方法をとる
  private static final long BASE_ALLOCATION_ID = Long.MAX_VALUE / 2 + RMContainerMigrationService.class.hashCode();
  
  private final RMContext rmContext;
  private final Map<ApplicationAttemptId, Long> migrationAttempts;
  
  public RMContainerMigrationService(RMContext rmContext) {
    super(RMContainerMigrationService.class.getName());
    this.rmContext = rmContext;
    this.migrationAttempts = new HashMap<>();
  }

  void move(RMContainer rmSourceContainer, RMNode rmSourceNode,
      RMNode rmDestinationNode) throws YarnException {
    ApplicationAttemptId applicationAttemptId =
        rmSourceContainer.getApplicationAttemptId();
    Container sourceContainer = rmSourceContainer.getContainer();
    ContainerId sourceContainerId = rmSourceContainer.getContainerId();
    long migrationAttempt;
    synchronized (migrationAttempts) {
      migrationAttempt = migrationAttempts.getOrDefault(applicationAttemptId, 0L);
      migrationAttempts.put(applicationAttemptId, migrationAttempt + 1L);
    }
    long allocationId = BASE_ALLOCATION_ID + migrationAttempt;
    
    // TODO 移行先のコンテナを確保する
    String destinationHost = rmDestinationNode.getHostName();
    String destinationRack = rmDestinationNode.getRackName();
    Priority priority = sourceContainer.getPriority();
    Resource capability = sourceContainer.getResource();
    ResourceRequest nodeLevelRequest = ResourceRequest.newInstance(
        priority, destinationHost, capability, 1, false);
    nodeLevelRequest.setAllocationRequestId(allocationId);
    ResourceRequest rackLevelRequest = ResourceRequest.newInstance(
        priority, destinationRack, capability, 1, false);
    rackLevelRequest.setAllocationRequestId(allocationId);
    ResourceRequest anyLevelRequest = ResourceRequest.newInstance(
        priority, ResourceRequest.ANY, capability, 1, false);
    anyLevelRequest.setAllocationRequestId(allocationId);
    List<ResourceRequest> ask = Arrays.asList(
        nodeLevelRequest, rackLevelRequest, anyLevelRequest);
    LOG.info(ask);
    Allocation allocation = this.rmContext.getScheduler().allocate(
        applicationAttemptId, new ArrayList<ResourceRequest>(ask), null,
        new ArrayList<ContainerId>(), null, null, new ContainerUpdates());
    RMContainer rmDestinationContainer = null;
    for (int retry = 0; retry < 30 && rmDestinationContainer == null; ++retry) {
      Collection<RMContainer> containers = this.rmContext.getScheduler()
          .getSchedulerAppInfo(applicationAttemptId).getLiveContainers();
      LOG.info(containers);
      for (RMContainer container : containers) {
        long aid = container.getContainer().getAllocationRequestId();
        if (Long.compare(allocationId, aid) == 0) {
          ContainerId containerId = container.getContainerId();
          rmDestinationContainer = this.rmContext.getScheduler()
              .getRMContainer(containerId);
          assert Objects.equals(container, rmDestinationContainer);
          break;
        }
      }
      if(rmDestinationContainer != null) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error(e);
        break;
      }
    }
    if (rmDestinationContainer == null) {
      LOG.info(allocation.toString());
      String description = String.format(
          "moveContainer: destination container allocation failed (appAttemptId=%s)",
          applicationAttemptId.toString());
      LOG.error(description);
      throw new YarnException(description);
    }
    LOG.info(rmDestinationContainer);
    // TODO リストア リクエストを送信する
    
    // TODO チェックポイント リクエストを送信する
    int destinationPort = PORT;
    NodeId sourceNodeId = rmSourceNode.getNodeID();
    ContainerCheckpointRequest checkpointRequest =
        ContainerCheckpointRequest.newInstance(sourceContainerId,
            destinationHost, destinationPort);
    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMNodeContainerCheckpointEvent(sourceNodeId, checkpointRequest));
    // TODO 移行元のコンテナを終了する
    
  }
}
