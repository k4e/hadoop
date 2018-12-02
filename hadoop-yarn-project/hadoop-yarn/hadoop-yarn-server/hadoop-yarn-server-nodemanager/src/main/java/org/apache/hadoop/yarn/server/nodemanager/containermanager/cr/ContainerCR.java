package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerCheckpointResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerRestoreResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ContainerCheckpointResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class ContainerCR extends AbstractService
    implements EventHandler<ContainerCREvent> {

  private final static Logger LOG = LoggerFactory.getLogger(ContainerCR.class);
  private final static String TMP = "tmp";
  private final static String CIMG_DIR = TMP + "/cimg";
  private final static String RIMG_DIR = TMP + "/rimg";
  private final static String CTX_BIN = "yarn-cr.ctx.bin";
  
  class CheckpointAndTransport implements Runnable {
    private final Container container;
    private final ContainerId containerId;
    private final String processId;
    private final ContainerCheckpointRequest request;
    public CheckpointAndTransport(Container container, String processId,
        ContainerCheckpointRequest request) {
      this.container = container;
      this.containerId = container.getContainerTokenIdentifier()
          .getContainerID();
      this.processId = processId;
      this.request = request;
    }
    @Override
    public void run() {
      long id = request.getId();
      String imagesDir = getPath(checkpointDirectory, containerId.toString(),
          Long.valueOf(id).toString());
      if (!makeDirectory(imagesDir)) {
        onFailure(id, String.format("Make directory % failed", imagesDir));
        return;
      }
      ContainerLaunchContextProto ctxProto =
          ((ContainerLaunchContextPBImpl)container.getLaunchContext())
          .getProto();
      try {
        String ctxBinPath = getPath(imagesDir, CTX_BIN);
        BufferedOutputStream ctxFileOut = new BufferedOutputStream(
            new FileOutputStream(ctxBinPath));
        ctxFileOut.write(ctxProto.toByteArray());
        ctxFileOut.close();
      } catch (IOException e) {
        onFailure(id, e.toString());
        return;
      }
      ProcessBuilder processBuilder = new ProcessBuilder(
          "criu", "dump", "--tree", processId, "--images-dir", imagesDir,
          "--leave-stopped", "--shell-job");
      processBuilder.redirectErrorStream(true);
      int exitValue;
      try {
        Process process = processBuilder.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
          LOG.info("[criu dump] " + line);
        }
        reader.close(); // 場所はここで大丈夫？
        exitValue = process.waitFor();
        reader.close();
      } catch (IOException | InterruptedException e) {
        onFailure(id, e.toString());
        return;
      }
      if (exitValue != 0) {
        onFailure(id, String.format("criu returned %d", exitValue));
        return;
      }
      // TODO イメージファイルの送信
      onSuccess(id);
    }
    private void onSuccess(long id) {
      onCheckpointSuccess(id, processId, container.getUser(), containerId);
    }
    private void onFailure(long id, String msg) {
      onCheckpointFailure(id, msg, processId, container.getUser(),
          containerId);
    }
  }
  
  class RestoreByTransport implements Runnable {
    private final ContainerRestoreRequest request;
    public RestoreByTransport(ContainerRestoreRequest request) {
      this.request = request;
    }
    @Override
    public void run() {
      long id = request.getId();
      String imagesDir = getPath(restoreDirectory,
          request.getSourceContainerId().toString(),
          Long.valueOf(id).toString());
      // TODO イメージファイルの受信
      ByteArrayOutputStream ctxByteOut = new ByteArrayOutputStream();
      try {
        String ctxBinPath = getPath(imagesDir, CTX_BIN);
        BufferedInputStream ctxFileIn = new BufferedInputStream(
            new FileInputStream(ctxBinPath));
        int data;
        while ((data = ctxFileIn.read()) >= 0) {
          ctxByteOut.write(data);
        }
        ctxFileIn.close();
      } catch (IOException e) {
        onFailure(id, e.toString());
        return;
      }
      ContainerLaunchContextProto ctxProto;
      try {
        ctxProto = ContainerLaunchContextProto
            .parseFrom(ctxByteOut.toByteArray());
      } catch (InvalidProtocolBufferException e) {
        onFailure(id, e.toString());
        return;
      }
      ContainerLaunchContextPBImpl ctx = new ContainerLaunchContextPBImpl(
          ctxProto);
      List<String> commands = Collections.singletonList(String.format(
          "criu restore --images-dir %d --restore-sibling", imagesDir));
      ctx.setCommands(commands);
      StartContainerRequest startContainerRequest = StartContainerRequest
          .newInstance(ctx, request.getContainerToken());
      StartContainersRequest startContainersRequest = StartContainersRequest
          .newInstance(Collections.singletonList(startContainerRequest));
      try {
        nmContext.getContainerManager().startContainers(startContainersRequest);
      } catch (YarnException | IOException e) {
        onFailure(id, e.toString());
        return;
      }
      onSuccess(id);
    }
    private void onSuccess(long id) {
      onRestoreSuccess(id,
          request.getContainerId(), request.getSourceContainerId());
    }
    private void onFailure(long id, String msg) {
      onRestoreFailure(id, msg, 
          request.getContainerId(), request.getSourceContainerId());
    }
  }
  
  private final Context nmContext;
  private final Dispatcher dispatcher;
  private final String checkpointDirectory;
  private final String restoreDirectory;
  private final LinkedList<ContainerCheckpointResponse> checkpointResponseQueue;
  private final LinkedList<ContainerRestoreResponse> restoreResponseQueue;
  
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
    this.checkpointResponseQueue = new LinkedList<>();
    this.restoreResponseQueue = new LinkedList<>();
  }
  
  @Override
  public void handle(ContainerCREvent event) {
    switch (event.getType()) {
    case CHECKPOINT:
      ContainerCRCheckpointEvent checkpointEvent =
          (ContainerCRCheckpointEvent)event;
      checkpointAndTransport(checkpointEvent.getContainer(),
          checkpointEvent.getProcessId(), checkpointEvent.getRequest());
      break;
    case RESTORE:
      ContainerCRRestoreEvent restoreEvent = (ContainerCRRestoreEvent)event;
      restoreByTransport(restoreEvent.getRestoreRequest());
      break;
    }
  }
  
  public List<ContainerCheckpointResponse> getLastCheckpointResponses() {
    List<ContainerCheckpointResponse> responses;
    synchronized (this.checkpointResponseQueue) {
      responses = new ArrayList<>(this.checkpointResponseQueue);
      this.checkpointResponseQueue.clear();
    }
    return responses;
  }
  
  public List<ContainerRestoreResponse> getLastRestoreResponses() {
    List<ContainerRestoreResponse> responses;
    synchronized (this.restoreResponseQueue) {
      responses = new ArrayList<>(this.restoreResponseQueue);
      this.restoreResponseQueue.clear();
    }
    return responses;
  }
  
  private void checkpointAndTransport(Container container, String processId,
      ContainerCheckpointRequest request) {
    CheckpointAndTransport cat = new CheckpointAndTransport(
        container, processId, request);
    Thread thread = new Thread(cat);
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.error(e.toString());
    }
  }
  
  private void restoreByTransport(ContainerRestoreRequest request) {
    RestoreByTransport rbt = new RestoreByTransport(request);
    Thread thread = new Thread(rbt);
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.error(e.toString());
    }
  }
  
  private void addCheckpointResponse(long id, int status) {
    synchronized (this.checkpointResponseQueue) {
      this.checkpointResponseQueue.add(
          ContainerCheckpointResponse.newInstance(id, status));
    }
  }
  
  private void addRestoreResponse(long id, int status) {
    synchronized (this.restoreResponseQueue) {
      this.restoreResponseQueue.add(
          ContainerRestoreResponse.newInstance(id, status));
    }
  }
  
  private String getPath(String p, String... d) {
    StringBuffer sb = new StringBuffer();
    for (String e : d) {
      sb.append("/" + StringUtils.strip(e, "/"));
    }
    return StringUtils.stripEnd(p, "/") + sb.toString();
  }
  
  private boolean makeDirectory(String path) {
    File dir = new File(path);
    return (dir.isDirectory() || dir.mkdirs());
  }

  private void onCheckpointSuccess(long id, String processId, String user,
      ContainerId containerId) {
    String diagnostics = String.format(
        "Checkpointed process %s as user %s for container %s, result = success",
        processId, user, containerId.toString());
    addCheckpointResponse(id, ContainerCheckpointResponse.SUCCESS);
    dispatcher.getEventHandler().handle(
        new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
  }
  
  private void onCheckpointFailure(long id, String msg, String processId,
      String user, ContainerId containerId) {
    String diagnostics = String.format(
        "Checkpointed process %s as user %s for container %s, result = failure",
        processId, user, containerId.toString());
    LOG.error("CheckpointAndTransport: " + msg);
    addCheckpointResponse(id, ContainerCheckpointResponse.FAILURE);
    dispatcher.getEventHandler().handle(
        new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
  }

  private void onRestoreSuccess(long id, ContainerId destinationContainerId,
      ContainerId sourceContainerId) {
    String diagnostics = String.format(
        "Restored container as %s from source container %s, result = success",
        destinationContainerId.toString(), sourceContainerId.toString());
    addRestoreResponse(id, ContainerRestoreResponse.SUCCESS);
    dispatcher.getEventHandler().handle(
        new ContainerDiagnosticsUpdateEvent(
            destinationContainerId, diagnostics));
  }
  
  private void onRestoreFailure(long id, String msg,
      ContainerId destinationContainerId, ContainerId sourceContainerId) {
    String diagnostics = String.format(
        "Restored container as %s from source container %s, result = failure",
        destinationContainerId.toString(), sourceContainerId.toString());
    LOG.error("RestoreByTransport: " + msg);
    addRestoreResponse(id, ContainerRestoreResponse.FAILURE);
    dispatcher.getEventHandler().handle(
        new ContainerDiagnosticsUpdateEvent(
            destinationContainerId, diagnostics));
  }
}
