package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;
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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCRFinishRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCRType;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class RMContainerMigrationService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(RMContainerMigrationService.class);
  private static final int RETRY_LIMIT = 300;
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
      RMNode rmDestinationNode) throws YarnException, IOException {
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
    String sourceHost = rmSourceNode.getHostName();
    
    // 移行先のコンテナをアロケートする
    String destinationHost = rmDestinationNode.getHostName();
    String destinationRack = rmDestinationNode.getRackName();
    Priority priority = sourceContainer.getPriority();
    Resource capability = sourceContainer.getResource();
    ExecutionType execType = sourceContainer.getExecutionType();
    ResourceRequest nodeLevelRequest = ResourceRequest.newInstance(
        priority, destinationHost, capability, 1, false);
    nodeLevelRequest.setAllocationRequestId(allocationId);
    nodeLevelRequest.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(execType, true));
    ResourceRequest rackLevelRequest = ResourceRequest.newInstance(
        priority, destinationRack, capability, 1, false);
    rackLevelRequest.setAllocationRequestId(allocationId);
    rackLevelRequest.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(execType, true));
    ResourceRequest anyLevelRequest = ResourceRequest.newInstance(
        priority, ResourceRequest.ANY, capability, 1, false);
    anyLevelRequest.setAllocationRequestId(allocationId);
    anyLevelRequest.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(execType, true));
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
        Thread.sleep(100);
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
    
    // 2 つのノードの ContainerManager プロキシを取得
    NodeId sourceNodeId = rmSourceNode.getNodeID();
    NodeId destinationNodeId = rmDestinationNode.getNodeID();
    ContainerManagementProtocol sourceContainerManager
        = getContainerMgrProxy(sourceNodeId, applicationAttemptId);
    ContainerManagementProtocol destinationContainerManager
        = getContainerMgrProxy(destinationNodeId, applicationAttemptId);
    // チェックポイント リクエストを送信する
    ContainerCheckpointRequest checkpointRequest =
        ContainerCheckpointRequest.newInstance(migrationId, sourceContainerId,
        destinationHost);
    ContainerCheckpointResponse checkpointResponse =
        sourceContainerManager.checkpointContainer(checkpointRequest);
    // リストア リクエストを送信する
    ContainerId destinationContainerId = rmDestinationContainer
        .getContainerId();
    ContainerRestoreResponse restoreResponse = null;
    if (checkpointResponse.getStatus() == ContainerCheckpointResponse.SUCCESS
        && checkpointResponse.hasDirectory()) {
      Token destinationContainerToken = rmDestinationContainer.getContainer()
          .getContainerToken();
      ContainerRestoreRequest restoreRequest = ContainerRestoreRequest
          .newInstance(migrationId, destinationContainerId,
              destinationContainerToken, sourceContainerId, sourceHost,
              checkpointResponse.getDirectory());
      restoreResponse = destinationContainerManager.restoreContainer(restoreRequest);
    }
    // 終了処理を行う
    boolean completing =
        (checkpointResponse != null && restoreResponse != null
        && checkpointResponse.getStatus() == ContainerCheckpointResponse.SUCCESS
        && restoreResponse.getStatus() == ContainerRestoreResponse.SUCCESS);
    ContainerCRFinishRequest sourceFinishRequest = ContainerCRFinishRequest
        .newInstance(migrationId, ContainerCRType.CHECKPOINT,
            sourceContainerId, destinationContainerId, completing);
    ContainerCRFinishRequest destinationFinishRequest = ContainerCRFinishRequest
        .newInstance(migrationId, ContainerCRType.RESTORE,
            sourceContainerId, destinationContainerId, completing);
    if (completing) {
      // TODO 成功時の後処理
    }
    sourceContainerManager.crFinish(sourceFinishRequest);
    destinationContainerManager.crFinish(destinationFinishRequest);
  }
  
  public boolean isWaitingAllocation() {
    return !this.waitingContainers.isEmpty();
  }
  
  public void notifyAllocation(ApplicationAttemptId appAttemptId,
      Allocation allocation) {
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
  
  public void removeMigratingContainersFromAllocateResponse(
      ApplicationAttemptId appAttemptId, AllocateResponse response) {
    ArrayList<Container> containers = new ArrayList<>(
        response.getAllocatedContainers());
    synchronized (this.allocatedContainers) {
      containers.removeIf((c) -> {
        Pair<ApplicationAttemptId, Long> key = Pair.of(
            appAttemptId, c.getAllocationRequestId());
        return this.allocatedContainers.containsKey(key);
      });
    }
    response.setAllocatedContainers(containers);
  }
  
  private ContainerManagementProtocol getContainerMgrProxy(NodeId nodeId,
      ApplicationAttemptId attemptId) throws IOException {
    InetSocketAddress address = NetUtils
        .createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
    YarnRPC rpc = getYarnRPC();
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    String user = rmContext.getRMApps().get(attemptId.getApplicationId())
        .getUser();
    Token token = rmContext.getNMTokenSecretManager().createNMToken(
        attemptId, nodeId, user);
    currentUser.addToken(ConverterUtils.convertFromYarn(token, address));
    return NMProxy.createNMProxy(rmContext.getYarnConfiguration(),
        ContainerManagementProtocol.class, currentUser, rpc, address);
  }
  
  private YarnRPC getYarnRPC() {
    // TODO: Don't create again and again.
    return YarnRPC.create(this.rmContext.getYarnConfiguration());
  }
}
