package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerCR extends AbstractService
    implements EventHandler<ContainerCREvent> {

  private final static Logger LOG = LoggerFactory.getLogger(ContainerCR.class);
  private final static String IMGDIR = "tmp/crimg";
  private final static long WAIT_TIMEOUT_MS = 30000;
  
  class Checkpoint {
    private final Container container;
    private final String processId;
    private final ContainerMigrationProcessRequest request;
    
    public Checkpoint(Container container, String processId,
        ContainerMigrationProcessRequest request) {
      this.container = container;
      this.processId = processId;
      this.request = request;
    }
    
    public void execute() {
      final long id = request.getId();
      ContainerId containerId = container.getContainerTokenIdentifier()
          .getContainerID();
      final int port = request.getDestinationPort();
      final String address = request.getDestinationAddress();
      final String user = container.getUser();
      final ContainerLaunchContext ctx = container.getLaunchContext();
      try {
        executeInternal(id, containerId, address, port);
        onSuccess(id, ctx, processId, containerId, user, address);
      } catch(CRException | IOException e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
        onFailure(id, processId, containerId, user, address, e.toString());
      }
    }
    
    private void executeInternal(final long id, final ContainerId containerId,
        final String address, final int port) throws CRException, IOException {
      ProcessBuilder processBuilder = new ProcessBuilder(
          "criu", "dump", "--page-server", "--address", address, "--port",
          Integer.valueOf(port).toString(),
          "--tree", processId, "--leave-stopped", "--shell-job");
      processBuilder.redirectErrorStream(true);
      int exitValue;
      try {
        Process process = processBuilder.start();
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
          LOG.info("criu dump: " + line);
        }
        exitValue = process.waitFor();
        reader.close(); // 場所はここで大丈夫？
      } catch (InterruptedException e) {
        throw new CRException(e.toString());
      }
      if (exitValue != 0) {
        throw new CRException(
            String.format("criu dump returned %d", exitValue));
      }
    }
    
    private void onSuccess(long id, ContainerLaunchContext ctx,
        String processId, ContainerId containerId, String user, String address) {
      setCheckpointResponse(
          request, ContainerMigrationProcessResponse.SUCCESS, ctx);
      String diagnostics = String.format(
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, imgdir: %s, result: success",
          id, containerId, processId, user, address);
      LOG.info(diagnostics);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
    }
    
    private void onFailure(long id, String processId, ContainerId containerId,
        String user, String address, String msg) {
      setCheckpointResponse(
          request, ContainerMigrationProcessResponse.FAILURE, null);
      String diagnostics = String.format(
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, imgdir: %s, result: failure",
          id, containerId, processId, user, address);
      LOG.error(diagnostics + "; " + msg);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
    }
  }
  
  class OpenPageServer {
    private final ContainerMigrationProcessRequest request;
    
    public OpenPageServer(ContainerMigrationProcessRequest request) {
      this.request = request;
    }
    
    public void execute() {
      long id = request.getId();
      final ContainerId sourceContainerId = request.getSourceContainerId();
      final int destinationPort = request.getDestinationPort();
      final String imagesDir = getImagesDir(id, sourceContainerId);
      try {
        executeInternal(destinationPort, imagesDir);
        onSuccess(id, destinationPort, imagesDir);
      } catch(CRException | IOException e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
        onFailure(id, destinationPort, imagesDir, e.toString());
      }
    }
    
    private void executeInternal(int port, String imagesDir)
        throws CRException, IOException {
      makeDirectory(imagesDir);
      ProcessBuilder processBuilder = new ProcessBuilder(
          "criu", "page-server", "--images-dir", imagesDir,
          "--port", Integer.valueOf(port).toString());
      processBuilder.redirectErrorStream(true);
      int exitValue;
      try {
        Process process = processBuilder.start();
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
          LOG.info("criu dump: " + line);
        }
        exitValue = process.waitFor();
        reader.close(); // 場所はここで大丈夫？
      } catch (InterruptedException e) {
        throw new CRException(e);
      }
      if (exitValue != 0) {
        throw new CRException(
            String.format("criu page-server returned %d", exitValue));
      } 
    }
    
    private void onSuccess(long id, int port, String imagesDir) {
      setOpenPageServerResponse(
          request, ContainerMigrationProcessResponse.SUCCESS, imagesDir);
      String diagnostics = String.format(
          "OpenPageServer: id: %d, port: %d, imgdir: %s, result: success",
          id, port, imagesDir);
      LOG.info(diagnostics);
    }
    
    private void onFailure(long id, int port, String imagesDir, String msg) {
      setOpenPageServerResponse(
          request, ContainerMigrationProcessResponse.FAILURE, null);
      String diagnostics = String.format(
          "OpenPageServer: id: %d, port: %d, imgdir: %s, result: failure",
          id, port, imagesDir);
      LOG.error(diagnostics + "; " + msg);
    }
  }
  
  private final Context nmContext;
  private final Dispatcher dispatcher;
  private final String imagesDirHome;
  private final ConcurrentHashMap<Pair<Long, Integer>, Pair<Integer, ContainerLaunchContext> > checkpointStatusStore;
  private final ConcurrentHashMap<Pair<Long, Integer>, Pair<Integer, String> > openPageServerStatusStore;
  
  public ContainerCR(Context nmContext, Dispatcher dispatcher) {
    super(ContainerCR.class.getName());
    this.nmContext = nmContext;
    this.dispatcher = dispatcher;
    String hadoopHome = System.getenv(Shell.ENV_HADOOP_HOME);
    if (hadoopHome != null) {
      hadoopHome = StringUtils.strip(hadoopHome, "/");
      this.imagesDirHome = String.format("/%s/%s", hadoopHome, IMGDIR);
    } else {
      this.imagesDirHome = String.format("/%s", IMGDIR);
    }
    this.checkpointStatusStore = new ConcurrentHashMap<>();
    this.openPageServerStatusStore = new ConcurrentHashMap<>();
  }
  
  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
  }
  
  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }
  
  @Override
  public void handle(ContainerCREvent event) {
    switch (event.getType()) {
    case CHECKPOINT:
      ContainerCRCheckpointEvent checkpointEvent =
          (ContainerCRCheckpointEvent)event;
      executeCheckpoint(checkpointEvent.getContainer(),
          checkpointEvent.getProcessId(), checkpointEvent.getRequest());
      break;
    case OPEN_PAGE_SERVER:
      ContainerCROpenPageServerEvent restoreEvent =
          (ContainerCROpenPageServerEvent)event;
      executeOpenPageServer(restoreEvent.getRequest());
      break;
    }
  }
  
  public ContainerMigrationProcessResponse getCheckpointResponse(
      ContainerMigrationProcessRequest request) {
    long id = request.getId();
    Pair<Long, Integer> key = Pair.of(id, request.hashCode());
    Pair<Integer, ContainerLaunchContext> value = waitAndGet(
        key, this.checkpointStatusStore);
    if (value != null) {
      int status = value.getLeft();
      ContainerMigrationProcessResponse response =
          ContainerMigrationProcessResponse.newInstance(id, status);
      if (status == ContainerMigrationProcessResponse.SUCCESS &&
          value.getRight() != null) {
        response.setContainerLaunchContext(value.getRight());
      }
      return response;
    } else {
      return ContainerMigrationProcessResponse.newInstance(
          id, ContainerMigrationProcessResponse.FAILURE);
    }
  }
  
  public ContainerMigrationProcessResponse getOpenPageServerResponse(
      ContainerMigrationProcessRequest request) {
    long id = request.getId();
    Pair<Long, Integer> key = Pair.of(id, request.hashCode());
    Pair<Integer, String> value = waitAndGet(key, this.openPageServerStatusStore);
    if (value != null) {
      int status = value.getLeft();
      ContainerMigrationProcessResponse response =
          ContainerMigrationProcessResponse.newInstance(id, status);
      if (status == ContainerMigrationProcessResponse.SUCCESS &&
          value.getRight() != null) {
        response.setImagesDir(value.getRight());
      }
      return response;
    } else {
      return ContainerMigrationProcessResponse.newInstance(
          id, ContainerMigrationProcessResponse.FAILURE);
    }
  }
  
  private void executeCheckpoint(Container container, String processId,
      ContainerMigrationProcessRequest request) {
    Checkpoint checkpoint = new Checkpoint(container, processId, request);
    checkpoint.execute();
  }
  
  private void executeOpenPageServer(ContainerMigrationProcessRequest request) {
    OpenPageServer restore = new OpenPageServer(request);
    restore.execute();
  }
  
  private void setCheckpointResponse(ContainerMigrationProcessRequest request,
      int status, ContainerLaunchContext ctx) {
    Pair<Long, Integer> key = Pair.of(request.getId(), request.hashCode());
    Pair<Integer, ContainerLaunchContext> value = Pair.of(status, ctx);
    synchronized (this.checkpointStatusStore) {
      this.checkpointStatusStore.put(key, value);
      this.checkpointStatusStore.notifyAll();
    }
  }
  
  private void setOpenPageServerResponse(ContainerMigrationProcessRequest request,
      int state, String imagesDir) {
    Pair<Long, Integer> key = Pair.of(request.getId(), request.hashCode());
    Pair<Integer, String> value = Pair.of(state, imagesDir);
    synchronized (this.openPageServerStatusStore) {
      this.openPageServerStatusStore.put(key, value);
      this.openPageServerStatusStore.notifyAll();
    }
  }
  
  private String getRemotePath(String host, String path) {
    return host + ":" + path;
  }
  
  private String getPath(String p, String... d) {
    StringBuffer sb = new StringBuffer();
    for (String e : d) {
      sb.append("/" + StringUtils.strip(e, "/"));
    }
    return StringUtils.stripEnd(p, "/") + sb.toString();
  }
  
  private String getImagesDir(long id, ContainerId containerId) {
    return getPath(imagesDirHome,
        String.format("%d_%s", id, containerId.toString()));
  }
  
  private boolean makeDirectory(String path) {
    File dir = new File(path);
    return (dir.isDirectory() || dir.mkdirs());
  }
  
  private <K, V> V waitAndGet(K key, Map<K, V> cncrntMap) {
    long waitStart = System.currentTimeMillis();
    synchronized (cncrntMap) {
      while (!cncrntMap.containsKey(key)) {
        long waitMillis = WAIT_TIMEOUT_MS - (System.currentTimeMillis() - waitStart);
        if (waitMillis <= 0) {
          break;
        }
        try {
          cncrntMap.wait(waitMillis);
        } catch (InterruptedException e) {
          LOG.error(e.toString());
          break;
        }
      }
    }
    return cncrntMap.get(key);
  }
}
