package org.apache.hadoop.yarn.server.nodemanager.containermanager.cr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
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

import com.github.fracpete.processoutput4j.output.CollectingProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class ContainerCheckpointRestoreService extends AbstractService
    implements EventHandler<ContainerCREvent> {

  private final static Logger LOG = LoggerFactory.getLogger(ContainerCheckpointRestoreService.class);
  private final static String CONFIGURATION_FILE = "etc/hadoop/migration-settings.xml";
  private final static String IMG_SRC_DIR = "tmp/imgsrc";
  private final static String IMG_DST_DIR = "tmp/imgdst";
  private final static String PAGE_SERVER_LOG = "criu.pgsv.log";
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
      final int port = request.getPort();
      final String address = request.getAddress();
      final String imagesDirSrc = getImagesSrcDst(id, containerId);
      final String imagesDirDst = request.getImagesDir();
      final String user = container.getUser();
      final ContainerLaunchContext ctx = container.getLaunchContext();
      try {
        executeInternal(id, containerId, address, port, imagesDirSrc, imagesDirDst);
        onSuccess(id, ctx, processId, containerId, user, address);
      } catch(CRException | IOException e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
        onFailure(id, processId, containerId, user, address, e.toString());
      }
    }
    
    private void executeInternal(final long id, final ContainerId containerId,
        final String address, final int port, final String imagesDirSrc,
        final String imagesDirDst) throws CRException, IOException {
      if (!makeDirectory(imagesDirSrc)) {
        throw new CRException("Make directory failred: " + imagesDirSrc);
      }
      Pair<String, String> loginCred = getLoginCredential(address);
      String username = loginCred.getLeft();
      String secret = loginCred.getRight();
      String actionScriptPath = getPath(imagesDirSrc, "yarn-criu-action-script.sh");
      String logDir = container.getLogDir();
      String workDir = container.getWorkDir();
      List<Pair<String, String> > srcDstDir = Arrays.asList(
          Pair.of(imagesDirSrc + "/", imagesDirDst),
          Pair.of(logDir + "/", logDir),
          Pair.of(workDir + "/", workDir));
      File actionScriptFile = createActionScript(srcDstDir, username, address,
          secret, actionScriptPath);
      ProcessBuilder processBuilder = new ProcessBuilder(
          "criu", "dump", "--page-server", "--images-dir", imagesDirSrc,
          "--address", address, "--port", Integer.valueOf(port).toString(),
          "--tree", processId, "--leave-stopped", "--tcp-established",
          "--shell-job", "--action-script", actionScriptFile.getCanonicalPath());
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
      // rsync(imagesDirSrc + "/", username, address, secret, imagesDirDst);
      // rsync(logDir + "/", username, address, secret, logDir);
      // rsync(workDir + "/", username, address, secret, workDir);
    }
    
    private File createActionScript(List<Pair<String, String> > srcDstDir,
        String username, String address, String secret, String filepath)
        throws IOException {
      List<String> lines = Lists.newArrayList();
      for(Pair<String, String> sd : srcDstDir) {
        String src = sd.getLeft();
        String dst = sd.getRight();
        String line = String.format("rsync -a -e\"ssh -i %s\" %s %s@%s:%s",
            secret, src, username, address, dst);
        lines.add(line);
      }
      File file = new File(filepath);
      file.createNewFile();
      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
      writer.write("if [ ${CRTOOLS_SCRIPT_ACTION} = \"post-dump\" ];");
      writer.newLine();
      writer.write("then");
      writer.newLine();
      for (String line : lines) {
        writer.write("  " + line + ";");
        writer.newLine();
      }
      writer.write("fi");
      writer.newLine();
      writer.close();
      file.setExecutable(true);
      return file;
    }

    private void rsync(String srcDir, String username, String address,
        String secret, String dstDir) throws CRException {
      String rsh = String.format("ssh -i %s", secret);
      String url = String.format("%s@%s:%s", username, address, dstDir);
      LOG.info(String.format("rsync %s %s", srcDir, url));
      RSync rsync = new RSync().archive(true).rsh(rsh).source(srcDir)
          .destination(url);
      try {
        CollectingProcessOutput rsyncOut = rsync.execute();
        LOG.info("rsync: " + rsyncOut.getStdOut());
        if (rsyncOut.getExitCode() != 0) {
          throw new CRException(String.format("rsync: returned %d: %s", 
              rsyncOut.getExitCode(), rsyncOut.getStdErr()));
        }
      } catch (Exception e) {
        throw new CRException(e);
      }
    }
    
    private void onSuccess(long id, ContainerLaunchContext ctx,
        String processId, ContainerId containerId, String user,
        String address) {
      setCheckpointResponse(
          request, ContainerMigrationProcessResponse.SUCCESS, ctx);
      String diagnostics = String.format(
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, addr: %s, result: success",
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
          "Checkpoint: id: %d, cid: %s, pid: %s, user: %s, addr: %s, result: failure",
          id, containerId, processId, user, address);
      LOG.error(diagnostics + "; " + msg);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
    }
  }
  
  class OpenReceiver {
    private final ContainerMigrationProcessRequest request;
    
    public OpenReceiver(ContainerMigrationProcessRequest request) {
      this.request = request;
    }
    
    public void execute() {
      long id = request.getId();
      final ContainerId sourceContainerId = request.getSourceContainerId();
      final int destinationPort = request.getPort();
      final String imagesDir = getImagesDirDst(id, sourceContainerId);
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
      File imagesDirFile = new File(imagesDir);
      imagesDirFile.setReadable(true, false);
      imagesDirFile.setWritable(true, false);
      imagesDirFile.setExecutable(true, false);
      File logFile = new File(getPath(imagesDir, PAGE_SERVER_LOG));
      logFile.createNewFile();
      ProcessBuilder processBuilder = new ProcessBuilder(
          "criu", "lazy-pages", "--images-dir", imagesDir, "--page-server",
          "--port", Integer.valueOf(port).toString());
      processBuilder.redirectErrorStream(true);
      processBuilder.redirectOutput(logFile);
      Process process = processBuilder.start();
      receivers.push(process);
    }
    
    private void onSuccess(long id, int port, String imagesDir) {
      setOpenReceiverResponse(
          request, ContainerMigrationProcessResponse.SUCCESS, imagesDir);
      String diagnostics = String.format(
          "OpenReceiver: id: %d, port: %d, imgdir: %s, result: success",
          id, port, imagesDir);
      LOG.info(diagnostics);
    }
    
    private void onFailure(long id, int port, String imagesDir, String msg) {
      setOpenReceiverResponse(
          request, ContainerMigrationProcessResponse.FAILURE, null);
      String diagnostics = String.format(
          "OpenReceiver: id: %d, port: %d, imgdir: %s, result: failure",
          id, port, imagesDir);
      LOG.error(diagnostics + "; " + msg);
    }
  }
  
  private final Context nmContext;
  private final Dispatcher dispatcher;
  private final String configurationPath;
  private final String imagesDirSrcHome;
  private final String imagesDirDstHome;
  private final Map<String, Pair<String, String> > loginCredentials;
  private final ConcurrentLinkedDeque<Process> receivers;
  private final ConcurrentHashMap<Pair<Long, Integer>, Pair<Integer, ContainerLaunchContext> > checkpointStatusStore;
  private final ConcurrentHashMap<Pair<Long, Integer>, Pair<Integer, String> > openReceiverStatusStore;
  private Pair<String, String> loginCredentialGlobal = null;
  
  public ContainerCheckpointRestoreService(Context nmContext, Dispatcher dispatcher) {
    super(ContainerCheckpointRestoreService.class.getName());
    this.nmContext = nmContext;
    this.dispatcher = dispatcher;
    String hadoopHome = System.getenv(Shell.ENV_HADOOP_HOME);
    if (hadoopHome != null) {
      hadoopHome = StringUtils.strip(hadoopHome, "/");
      this.configurationPath = String.format("/%s/%s", hadoopHome, CONFIGURATION_FILE);
      this.imagesDirSrcHome = String.format("/%s/%s", hadoopHome, IMG_SRC_DIR);
      this.imagesDirDstHome = String.format("/%s/%s", hadoopHome, IMG_DST_DIR);
    } else {
      this.configurationPath = String.format("/%s", CONFIGURATION_FILE);
      this.imagesDirSrcHome = String.format("/%s", IMG_SRC_DIR);
      this.imagesDirDstHome = String.format("/%s", IMG_DST_DIR);
    }
    this.loginCredentials = new HashMap<>();
    this.receivers = new ConcurrentLinkedDeque<>();
    this.checkpointStatusStore = new ConcurrentHashMap<>();
    this.openReceiverStatusStore = new ConcurrentHashMap<>();
  }
  
  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
    loadConfiguration();
  }
  
  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
    synchronized (this.receivers) {
      while (!this.receivers.isEmpty()) {
        Process process = this.receivers.pop();
        if (process.isAlive()) {
          process.destroy();
        }
      }
    }
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
    case OPEN_RECEIVER:
      ContainerCROpenReceiver restoreEvent =
          (ContainerCROpenReceiver)event;
      executeOpenReceiver(restoreEvent.getRequest());
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
  
  public ContainerMigrationProcessResponse getOpenReceiverResponse(
      ContainerMigrationProcessRequest request) {
    long id = request.getId();
    Pair<Long, Integer> key = Pair.of(id, request.hashCode());
    Pair<Integer, String> value = waitAndGet(key, this.openReceiverStatusStore);
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
  
  private void loadConfiguration() throws IOException, ConfigurationException {
    XMLConfiguration xmlConf = new XMLConfiguration();
    FileHandler fileHandler = new FileHandler(xmlConf);
    fileHandler.load(this.configurationPath);
    xmlConf.setExpressionEngine(new XPathExpressionEngine());
    List<HierarchicalConfiguration<ImmutableNode> > ftNodes =
        xmlConf.childConfigurationsAt("login/nodes");
    for(HierarchicalConfiguration<ImmutableNode> c : ftNodes) {
      String address = c.getString("address");
      String username = c.getString("username");
      String secret = c.getString("secret");
      LOG.info(String.format("Read login info (address=%s, username=%s)",
          address, username));
      Pair<String, String> pair = Pair.of(username, secret);
      if ("*".equals(address)) {
        this.loginCredentialGlobal = pair;
      } else {
        this.loginCredentials.put(address, pair);
      }
    }
  }
  
  private void executeCheckpoint(Container container, String processId,
      ContainerMigrationProcessRequest request) {
    Checkpoint checkpoint = new Checkpoint(container, processId, request);
    checkpoint.execute();
  }
  
  private void executeOpenReceiver(ContainerMigrationProcessRequest request) {
    OpenReceiver receiver = new OpenReceiver(request);
    receiver.execute();
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
  
  private void setOpenReceiverResponse(ContainerMigrationProcessRequest request,
      int state, String imagesDir) {
    Pair<Long, Integer> key = Pair.of(request.getId(), request.hashCode());
    Pair<Integer, String> value = Pair.of(state, imagesDir);
    synchronized (this.openReceiverStatusStore) {
      this.openReceiverStatusStore.put(key, value);
      this.openReceiverStatusStore.notifyAll();
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
  
  private String getImagesSrcDst(long id, ContainerId containerId) {
    return getPath(imagesDirSrcHome,
        String.format("%d_%s", id, containerId.toString()));
  }
  
  private String getImagesDirDst(long id, ContainerId containerId) {
    return getPath(imagesDirDstHome,
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
  
  private Pair<String, String> getLoginCredential(String address) throws CRException {
    if (this.loginCredentials.containsKey(address)) {
      return this.loginCredentials.get(address);
    } else if (this.loginCredentialGlobal != null) {
      return this.loginCredentialGlobal;
    } else {
      throw new CRException("No login info for address: " + address);
    }
  }
}
