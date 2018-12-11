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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
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

import com.github.fracpete.processoutput4j.output.CollectingProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;

public class ContainerCR extends AbstractService
    implements EventHandler<ContainerCREvent> {

  private final static int DEFAULT_MESSAGE_LISTENER_PORT = 11111;
  private final static long DEFAULT_TIMEOUT_MS = 60000;
  private final static long DEFAULT_INTERVAL_MS = 500;
  private final static Logger LOG = LoggerFactory.getLogger(ContainerCR.class);
  private final static Gson GSON = new Gson();
  private final static String CIMG_DIR = "tmp/cimg";
  private final static String RIMG_DIR = "tmp/rimg";
  private final static String CTX_BIN = "yarn-cr.ctx.bin";
  private final static String NULL_DIR = "<null>";
  
  static class CMessage implements Serializable {
    
    private final long id;
    private final int h;
    private final boolean sc;
    private final String dir;
    
    public CMessage(long id, int hash, boolean succeeded,
        String sourceImagesDir) {
      this.id = id;
      this.h = hash;
      this.sc = succeeded;
      this.dir = sourceImagesDir;
    }
    
    public long getId() { return id; }
    
    public int getHash() { return h; }
    
    public boolean getSucceeded() { return sc; }
    
    public String getSourceImagesDir() { return dir; }
  }
  
  class MessageListener extends Thread {
    
    private final ServerSocket serverSocket;
    public MessageListener(ServerSocket serverSocket) {
      this.serverSocket = serverSocket;
    }
    
    @Override
    public void run() {
      while (true) {
        Socket sock;
        try {
          sock = serverSocket.accept();
        } catch (SocketTimeoutException e) {
          continue;
        } catch (IOException e) {
          LOG.error(e.toString());
          return;
        }
        CMessage message;
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(
              sock.getInputStream()));
          message = GSON.fromJson(reader, CMessage.class);
          reader.close();
        } catch (Exception e) {
          LOG.error(e.toString());
          continue;
        }
        Pair<Long, Integer> key = Pair.of(message.getId(), message.getHash());
        String value = (message.getSucceeded()
            ? message.getSourceImagesDir() : NULL_DIR);
        sourceImagesDirStore.put(key, value);
      }
    }
  }
  
  class Checkpoint {
    private final Container container;
    private final String processId;
    private final ContainerCheckpointRequest request;
    
    public Checkpoint(Container container, String processId,
        ContainerCheckpointRequest request) {
      this.container = container;
      this.processId = processId;
      this.request = request;
    }
    
    public void execute() {
      final long id = request.getId();
      ContainerId containerId = container.getContainerTokenIdentifier()
          .getContainerID();
      String imagesDir = getImagesDir(checkpointDirectory, containerId,
          request.getId());
      try {
        executeInternal(id, containerId, imagesDir);
        onSuccess(id, processId, containerId, container.getUser(), imagesDir);
      } catch(CRException | IOException e) {
        onFailure(id, processId, containerId, container.getUser(), imagesDir,
            e.toString());
      }
    }
    
    private void executeInternal(final long id, final ContainerId containerId,
        final String imagesDir) throws CRException, IOException {
      if (!makeDirectory(imagesDir)) {
        throw new CRException(String.format("Make directory % failed", imagesDir));
      }
      ContainerLaunchContextProto ctxProto =
          ((ContainerLaunchContextPBImpl)container.getLaunchContext())
          .getProto();
      String ctxBinPath = getPath(imagesDir, CTX_BIN);
      BufferedOutputStream ctxFileOut = new BufferedOutputStream(
          new FileOutputStream(ctxBinPath));
      ctxFileOut.write(ctxProto.toByteArray());
      ctxFileOut.close();
      ProcessBuilder processBuilder = new ProcessBuilder(
          "criu", "dump", "--tree", processId, "--images-dir", imagesDir,
          "--leave-stopped", "--shell-job");
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
      sendMessage(id, containerId, imagesDir, request.getAddress(),
          DEFAULT_MESSAGE_LISTENER_PORT);
    }
    
    private void onSuccess(long id, String processId, ContainerId containerId,
        String user, String imagesDir) {
      setCheckpointResponse(request, ContainerCheckpointResponse.SUCCESS);
      String diagnostics = String.format(
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, imgdir: %s, result: success",
          id, containerId, processId, user, imagesDir);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
    }
    
    private void onFailure(long id, String processId, ContainerId containerId,
        String user, String imagesDir, String msg) {
      setCheckpointResponse(request, ContainerCheckpointResponse.FAILURE);
      String diagnostics = String.format(
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, imgdir: %s, result: failure",
          id, containerId, processId, user, imagesDir);
      LOG.error("ContainerCR.Checkpoint: " + msg);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
      try {
        sendMessage(id, containerId, null, request.getAddress(),
            DEFAULT_MESSAGE_LISTENER_PORT);
      } catch (IOException e) {
        LOG.error(e.toString());
      }
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
          throw new CRException("request.directory == null");
        }
        String remoteImagesDir = getRemotePath(sourceHost, sourceImagesDir);
        executeInternal(id, destinationContainerId, destinationImagesDir,
            sourceContainerId, remoteImagesDir);
        onSuccess(id, destinationContainerId, destinationImagesDir,
            sourceContainerId, sourceHost, sourceImagesDir);
      } catch(CRException | YarnException | IOException e) {
        onFailure(id, destinationContainerId, destinationImagesDir,
            sourceContainerId, sourceHost, sourceImagesDir, e.toString());
      }
    }
    
    private void executeInternal(final long id,
        final ContainerId destinationContainerId, final String imagesDir,
        final ContainerId sourceContainerId, final String remoteImagesDir)
        throws CRException, YarnException, IOException {
      RSync rsync = new RSync().source(remoteImagesDir).destination(imagesDir)
          .archive(true).delete(true);
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
          destinationContainerId.toString(), destinationImagesDir,
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
  private final ConcurrentHashMap<Pair<Long, Integer>, String> sourceImagesDirStore;
  private final ConcurrentHashMap<Pair<Long, Integer>, Integer> checkpointStateStore;
  private final ConcurrentHashMap<Pair<Long, Integer>, Integer> restoreStateStore;
  private ServerSocket serverSocket = null;
  private MessageListener messageListener = null;
  
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
    this.sourceImagesDirStore = new ConcurrentHashMap<>();
    this.checkpointStateStore = new ConcurrentHashMap<>();
    this.restoreStateStore = new ConcurrentHashMap<>();
  }
  
  @Override
  public void serviceStart() throws Exception {
    startMessageListener();
    super.serviceStart();
  }
  
  @Override
  public void serviceStop() throws Exception {
    stopMessageListener();
    super.serviceStop();
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
  
  public ContainerCheckpointResponse getCheckpointResponse(
      ContainerCheckpointRequest request, boolean failureIfNull) {
    long id = request.getId();
    Pair<Long, Integer> key = Pair.of(id, request.getContainerId().hashCode());
    Integer status = this.checkpointStateStore.get(key);
    if (status == null && failureIfNull) {
      status = ContainerCheckpointResponse.FAILURE;
    }
    if (status != null) {
      return ContainerCheckpointResponse.newInstance(id, status);
    } else {
      return null;
    }
  }
  
  public ContainerRestoreResponse getRestoreResponse(
      ContainerRestoreRequest request, boolean failureIfNull) {
    long id = request.getId();
    Pair<Long, Integer> key = Pair.of(id, request.getContainerId().hashCode());
    Integer status = this.restoreStateStore.get(key);
    if (status == null && failureIfNull) {
      status = ContainerRestoreResponse.FAILURE;
    }
    if (status != null) {
      return ContainerRestoreResponse.newInstance(id, status);
    } else {
      return null;
    }
  }
  
  private void startMessageListener() throws IOException, InterruptedException {
    if (isMessageListenerAlive()) {
      stopMessageListener();
    }
    this.serverSocket = new ServerSocket(DEFAULT_MESSAGE_LISTENER_PORT);
    this.messageListener = new MessageListener(this.serverSocket);
    this.messageListener.start();
  }
  
  private void stopMessageListener() throws IOException, InterruptedException {
    if (this.serverSocket != null) {
      this.serverSocket.close();
    }
    if (this.messageListener != null) {
      this.messageListener.join(1000);
      if (this.messageListener.isAlive()) {
        this.messageListener.destroy();
      }
    }
    this.serverSocket = null;
    this.messageListener = null;
  }
  
  private boolean isMessageListenerAlive() {
    return this.serverSocket != null && this.messageListener != null
        && this.messageListener.isAlive();
  }
  
  private void checkpointAndTransport(Container container, String processId,
      ContainerCheckpointRequest request) {
    if (!isMessageListenerAlive()) {
      try {
        startMessageListener();
      } catch (Exception e) {
        LOG.error(e.toString());
      }
    }
    Checkpoint checkpoint = new Checkpoint(
        container, processId, request);
    checkpoint.execute();
  }
  
  private void restoreByTransport(ContainerRestoreRequest request) {
    Restore restore = new Restore(request);
    restore.execute();
  }
  
  private void setCheckpointResponse(ContainerCheckpointRequest request,
      int status) {
    Pair<Long, Integer> key = Pair.of(
        request.getId(), request.getContainerId().hashCode());
    this.checkpointStateStore.put(key, status);
  }
  
  private void setRestoreResponse(ContainerRestoreRequest request,
      int state) {
    Pair<Long, Integer> key = Pair.of(
        request.getId(), request.getContainerId().hashCode());
    this.restoreStateStore.put(key, state);
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
  
  private void sendMessage(long id, ContainerId containerId, String imagesDir,
      String host, int port) throws IOException {
    int hash = containerId.hashCode();
    CMessage cmessage;
    if (imagesDir != null) {
      cmessage = new CMessage(id, hash, true, imagesDir);
    } else {
      cmessage = new CMessage(id, hash, false, NULL_DIR);
    }
    String json = GSON.toJson(cmessage);
    Socket socket = null;
    try {
      socket = new Socket(host, port);
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(socket.getOutputStream()));
      writer.write(json);
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }
  
  private String obtainSourceImagesDir(long id, ContainerId sourceContainerId)
      throws Exception {
    int hash = sourceContainerId.hashCode();
    Pair<Long, Integer> key = Pair.of(id, hash);
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start <= DEFAULT_TIMEOUT_MS) {
      String s = sourceImagesDirStore.get(key);
      if (s != null) {
        return (NULL_DIR.equals(s) ? null : s);
      }
      Thread.sleep(DEFAULT_INTERVAL_MS);
    }
    throw new Exception("Timeout getting message");
  }
}
