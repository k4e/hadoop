package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
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
  private static final int RETRY_LIMIT = 30;
  // TODO 確実にこのサービスがアロケートしたコンテナであることがわかるような方法をとる
  private static final long BASE_ALLOCATION_ID = Long.MAX_VALUE / 2 + RMContainerMigrationService.class.hashCode();
  
  private final RMContext rmContext;
  private final AtomicLong countMigration;
  private final Map<ApplicationAttemptId, Long> migrationAttempts;
  private final Set<Pair<ApplicationAttemptId, Long> > waitingContainers;
  private final ConcurrentHashMap<Pair<ApplicationAttemptId, Long>, ContainerId> allocatedContainers;
  
  public RMContainerMigrationService(RMContext rmContext) {
    super(RMContainerMigrationService.class.getName());
    this.rmContext = rmContext;
    this.countMigration = new AtomicLong();
    this.migrationAttempts = new HashMap<>();
    this.waitingContainers = Collections.synchronizedSet(new HashSet<>());
    this.allocatedContainers = new ConcurrentHashMap<>();
  }

  void move(RMContainer rmSourceContainer, RMNode rmSourceNode,
      RMNode rmDestinationNode) throws YarnException {
    long migrationId = this.countMigration.getAndIncrement();
    ApplicationAttemptId applicationAttemptId =
        rmSourceContainer.getApplicationAttemptId();
    Container sourceContainer = rmSourceContainer.getContainer();
    ContainerId sourceContainerId = rmSourceContainer.getContainerId();
    long migrationAttempt;
    synchronized (migrationAttempts) {
      migrationAttempt = migrationAttempts.getOrDefault(
          applicationAttemptId, 0L);
      migrationAttempts.put(applicationAttemptId, migrationAttempt + 1L);
    }
    long allocationId = BASE_ALLOCATION_ID + migrationAttempt;
    
    // 移行先のコンテナをアロケートする
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
    Pair<ApplicationAttemptId, Long> waitingContainer = Pair.of(
        applicationAttemptId, Long.valueOf(allocationId));
    this.waitingContainers.add(waitingContainer);
    RMContainer rmDestinationContainer = null;
    for (int t = 0; t < RETRY_LIMIT && rmDestinationContainer == null; ++t) {
      ContainerId containerId = this.allocatedContainers.get(waitingContainer);
      if(containerId != null) {
        rmDestinationContainer = this.rmContext.getScheduler()
            .getRMContainer(containerId);
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
      String description = String.format(
          "moveContainer: destination container allocation failed (appAttemptId=%s)",
          applicationAttemptId.toString());
      LOG.error(description);
      throw new YarnException(description);
    }
    // TODO 指定したノードでアロケートできなかった場合には破棄する
    LOG.info(rmDestinationContainer);
    // TODO リストア リクエストを送信する
    // チェックポイント リクエストを送信する
    int destinationPort = PORT;
    NodeId sourceNodeId = rmSourceNode.getNodeID();
    ContainerCheckpointRequest checkpointRequest =
        ContainerCheckpointRequest.newInstance(migrationId, sourceContainerId,
            destinationHost, destinationPort);
    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMNodeContainerCheckpointEvent(sourceNodeId, checkpointRequest));
    // TODO 移行元のコンテナを終了する
    
  }
  
  public boolean isWaitingAllocation() {
    return !this.waitingContainers.isEmpty();
  }
  
  public void notifyAllocation(ApplicationAttemptId appAttemptId, Allocation allocation) {
    for (Container container : allocation.getContainers()) {
      long allocReqId = container.getAllocationRequestId();
      Pair<ApplicationAttemptId, Long> key = Pair.of(
          appAttemptId, Long.valueOf(allocReqId));
      if (allocReqId != -1) {
        if(this.waitingContainers.remove(key)) {
          ContainerId containerId = container.getId();
          this.allocatedContainers.put(key, containerId);
        }
      }
    }
  }
}
