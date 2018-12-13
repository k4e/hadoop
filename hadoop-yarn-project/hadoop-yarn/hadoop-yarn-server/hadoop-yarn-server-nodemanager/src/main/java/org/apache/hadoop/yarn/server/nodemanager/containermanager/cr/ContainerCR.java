package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerMigrationProcessResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerRequestProto;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fracpete.processoutput4j.output.CollectingProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;

public class ContainerCR extends AbstractService
    implements EventHandler<ContainerCREvent> {

  private final static Logger LOG = LoggerFactory.getLogger(ContainerCR.class);
  private final static String CIMG_DIR = "tmp/cimg";
  private final static String RIMG_DIR = "tmp/rimg";
  private final static String CTX_BIN = "yarn-cr.ctx.bin";
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
      try {
        executeInternal(id, containerId, address, port);
        onSuccess(id, processId, containerId, container.getUser(), address);
      } catch(CRException | IOException e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
        onFailure(id, processId, containerId, container.getUser(), address,
            e.toString());
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
        throw new CRException(String.format("criu returned %d", exitValue));
      }
    }
    
    private void onSuccess(long id, String processId, ContainerId containerId,
        String user, String address) {
      setCheckpointResponse(request, 0, address);
      String diagnostics = String.format(
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, imgdir: %s, result: success",
          id, containerId, processId, user, address);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
    }
    
    private void onFailure(long id, String processId, ContainerId containerId,
        String user, String address, String msg) {
      setCheckpointResponse(request, -1, null);
      String diagnostics = String.format(
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, imgdir: %s, result: failure",
          id, containerId, processId, user, address);
      LOG.error("ContainerCR.Checkpoint: " + msg);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
    }
  }
  
  class Restore {
    private final ContainerRestoreRequest request;
    
    public Restore(ContainerRestoreRequest request) {
      this.request = request;
    }
    
    public void execute() {
      long id = request.getId();
      ContainerId destinationContainerId = request.getContainerId();
      ContainerId sourceContainerId = request.getSourceContainerId();
      String destinationImagesDir = getImagesDir(restoreDirectory,
          sourceContainerId, id);
      String sourceHost = request.getAddress();
      String sourceImagesDir = request.getDirectory();
      try {
        if (sourceImagesDir == null) {
          throw new CRException("sourceImagesDir == null");
        }
        String remoteImagesDir = getRemotePath(sourceHost, sourceImagesDir);
        executeInternal(id, destinationContainerId, destinationImagesDir,
            sourceContainerId, remoteImagesDir);
        onSuccess(id, destinationContainerId, destinationImagesDir,
            sourceContainerId, sourceHost, sourceImagesDir);
      } catch(CRException | YarnException | IOException e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
        onFailure(id, destinationContainerId, destinationImagesDir,
            sourceContainerId, sourceHost, sourceImagesDir, e.toString());
      }
    }
    
    private void executeInternal(final long id,
        final ContainerId destinationContainerId, final String imagesDir,
        final ContainerId sourceContainerId, final String remoteImagesDir)
        throws CRException, YarnException, IOException {
      makeDirectory(imagesDir);
      String sourceDir = StringUtils.stripEnd(remoteImagesDir, "/") + "/";
      String destinationDir = StringUtils.stripEnd(imagesDir, "/") + "/";
      RSync rsync = new RSync().source(sourceDir).destination(destinationDir)
          .archive(true).delete(true).verbose(true);
      try {
        CollectingProcessOutput rsyncOut = rsync.execute();
        LOG.info("rsync: " + rsyncOut.getStdOut());
        if (rsyncOut.getExitCode() != 0) {
          throw new CRException("rsync: " + rsyncOut.getStdErr());
        }
      } catch (Exception e1) {
        throw new CRException(e1);
      }
      ByteArrayOutputStream ctxByteOut = new ByteArrayOutputStream();
      BufferedInputStream ctxFileIn = null;
      try {
        String ctxBinPath = getPath(imagesDir, CTX_BIN);
        ctxFileIn = new BufferedInputStream(new FileInputStream(ctxBinPath));
        int data;
        while ((data = ctxFileIn.read()) >= 0) {
          ctxByteOut.write(data);
        }
      } finally {
        if (ctxFileIn != null) {
          ctxFileIn.close();
        }
      }
      ContainerLaunchContextProto ctxProto;
      try {
        ctxProto = ContainerLaunchContextProto
            .parseFrom(ctxByteOut.toByteArray());
      } catch (InvalidProtocolBufferException e) {
        throw new CRException(e);
      }
      ContainerLaunchContextPBImpl ctx = new ContainerLaunchContextPBImpl(
          ctxProto);
      List<String> commands = Collections.singletonList(String.format(
          "criu restore --images-dir %s --restore-sibling", imagesDir));
      ctx.setCommands(commands);
      StartContainerRequest startContainerRequest = StartContainerRequest
          .newInstance(ctx, request.getContainerToken());
      StartContainersRequest startContainersRequest = StartContainersRequest
          .newInstance(Collections.singletonList(startContainerRequest));
      nmContext.getContainerManager().startContainers(startContainersRequest);
    }
    
    private void onSuccess(long id, ContainerId destinationContainerId,
        String destinationImagesDir, ContainerId sourceContainerId,
        String sourceHost, String sourceImagesDir) {
      setRestoreResponse(request, ContainerRestoreResponse.SUCCESS);
      String diagnostics = String.format(
          "Restore: id: %d, dst_cid: %s, dst_imgdir: %s, src_cid: %s, src_host: %s, src_imgdir: %s, result: success",
          destinationContainerId.toString(), destinationImagesDir,
          sourceContainerId.toString(), sourceHost, sourceImagesDir);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(
              destinationContainerId, diagnostics));
    }
    
    private void onFailure(long id, ContainerId destinationContainerId,
        String destinationImagesDir, ContainerId sourceContainerId,
        String sourceHost, String sourceImagesDir, String msg) {
      setRestoreResponse(request, ContainerRestoreResponse.FAILURE);
      String diagnostics = String.format(
          "Restore: id: %d, dst_cid: %s, dst_imgdir: %s, src_cid: %s, src_host: %s, src_imgdir: %s, result: failure",
          id, destinationContainerId.toString(), destinationImagesDir,
          sourceContainerId.toString(), sourceHost, sourceImagesDir);
      LOG.error("Restore: " + msg);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(
              destinationContainerId, diagnostics));
    }
  }
  
  private final Context nmContext;
  private final Dispatcher dispatcher;
  private final String checkpointDirectory;
  private final String restoreDirectory;
  private final ConcurrentHashMap<Pair<Long, Integer>, Pair<Integer, String> > checkpointStatusStore;
  private final ConcurrentHashMap<Pair<Long, Integer>, Integer> restoreStatusStore;
  
  public ContainerCR(Context nmContext, Dispatcher dispatcher) {
    super(ContainerCR.class.getName());
    this.nmContext = nmContext;
    this.dispatcher = dispatcher;
    String hadoopHome = System.getenv(Shell.ENV_HADOOP_HOME);
    if (hadoopHome != null) {
      hadoopHome = StringUtils.strip(hadoopHome, "/");
      this.checkpointDirectory = String.format("/%s/%s", hadoopHome, CIMG_DIR);
      this.restoreDirectory = String.format("/%s/%s", hadoopHome, RIMG_DIR);
    } else {
      this.checkpointDirectory = String.format("/%s`", CIMG_DIR);
      this.restoreDirectory = String.format("/%s", RIMG_DIR);
    }
    this.checkpointStatusStore = new ConcurrentHashMap<>();
    this.restoreStatusStore = new ConcurrentHashMap<>();
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
    case RESTORE:
      ContainerCRRestoreEvent restoreEvent = (ContainerCRRestoreEvent)event;
      executeRestore(restoreEvent.getRestoreRequest());
      break;
    }
  }
  
  public ContainerMigrationProcessResponse getCheckpointResponse(
      ContainerMigrationProcessRequest request) {
    long id = request.getId();
    Pair<Long, Integer> key = Pair.of(id, request.getContainerId().hashCode());
    Pair<Integer, String> value = waitAndGet(key, this.checkpointStatusStore);
    int status;
    String directory;
    if (value != null) {
      status = value.getLeft();
      directory = value.getRight();
    } else {
      status = ContainerCheckpointResponse.FAILURE;
      directory = null;
    }
    ContainerCheckpointResponse response = ContainerCheckpointResponse
        .newInstance(id, status);
    if (directory != null) {
      response.setDirectory(directory);
    }
    return response;
  }
  
  public ContainerRestoreResponse getRestoreResponse(
      ContainerRestoreRequest request) {
    long id = request.getId();
    Pair<Long, Integer> key = Pair.of(id, request.getContainerId().hashCode());
    Integer status = waitAndGet(key, this.restoreStatusStore);
    if (status == null) {
      status = ContainerRestoreResponse.FAILURE;
    }
    return ContainerRestoreResponse.newInstance(id, status);
  }
  
  private void executeCheckpoint(Container container, String processId,
      ContainerCheckpointRequest request) {
    Checkpoint checkpoint = new Checkpoint(
        container, processId, request);
    checkpoint.execute();
  }
  
  private void executeRestore(ContainerRestoreRequest request) {
    Restore restore = new Restore(request);
    restore.execute();
  }
  
  private void setCheckpointResponse(ContainerCheckpointRequest request,
      int status, String directory) {
    Pair<Long, Integer> key = Pair.of(
        request.getId(), request.getContainerId().hashCode());
    Pair<Integer, String> value = Pair.of(status, directory);
    synchronized (this.checkpointStatusStore) {
      this.checkpointStatusStore.put(key, value);
      this.checkpointStatusStore.notifyAll();
    }
  }
  
  private void setRestoreResponse(ContainerRestoreRequest request,
      int state) {
    Pair<Long, Integer> key = Pair.of(
        request.getId(), request.getContainerId().hashCode());
    synchronized (this.restoreStatusStore) {
      this.restoreStatusStore.put(key, state);
      this.restoreStatusStore.notifyAll();
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
  
  private String getImagesDir(String imagesHome, ContainerId containerId,
      long id) {
    return getPath(
        imagesHome, containerId.toString(), Long.valueOf(id).toString());
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
