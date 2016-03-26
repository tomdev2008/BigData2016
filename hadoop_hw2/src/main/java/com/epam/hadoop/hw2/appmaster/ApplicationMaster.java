package com.epam.hadoop.hw2.appmaster;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.epam.hadoop.hw2.DSConstants;
import com.epam.hadoop.hw2.ResourcesUtils;
import com.epam.hadoop.hw2.constants.CliConstants;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import static com.epam.hadoop.hw2.CliUtils.param;

public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    public static enum DSEvent {
        DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
    }

    public static enum DSEntity {
        DS_APP_ATTEMPT, DS_CONTAINER
    }

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync amRMClient;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;

    // Application Attempt Id ( combination of attemptId and fail count )
    protected ApplicationAttemptId appAttemptID;

    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    // App Master configuration
    // No. of containers to run shell command on
    protected int numTotalContainers = 1;
    // Memory to request for the container on which the shell command will run
    private int containerMemory = 10;
    // VirtualCores to request for the container on which the shell command will run
    private int containerVirtualCores = 1;
    // Priority of the request
    private int requestPriority;

    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    protected AtomicInteger numRequestedContainers = new AtomicInteger();

    private volatile boolean done;

    private List<Thread> launchThreads = new ArrayList<Thread>();

    // Timeline Client
    private TimelineClient timelineClient;

    private String jarPath;
    private long jarPathLen;
    private long jarPathTime;

    private String input = "";
    private String output = "";

    private static final String appMasterJarPath = "AppMaster.jar";

    private FileSystem fileSystem;

    private AtomicInteger numberContainers = new AtomicInteger();
    private AtomicInteger numberCompletedContainers = new AtomicInteger();

    private FileBlocks fileBlocks = new FileBlocks();

    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            ApplicationMaster appMaster = new ApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    public ApplicationMaster() throws IOException {
        conf = new YarnConfiguration();
        fileSystem = FileSystem.get(conf);
    }

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        Options opts = new Options();

        opts.addOption("help", false, "Print usage");
        opts.addOption("input", true, "Input file");
        opts.addOption("output", true, "Output file");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException("No args specified for application master to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        Map<String, String> envs = System.getenv();

        ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
        appAttemptID = containerId.getApplicationAttemptId();


        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV+ " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        jarPath = envs.get(DSConstants.JARLOCATION);
        jarPathLen = Long.parseLong(envs.get(DSConstants.JARLOCATIONLEN));
        jarPathTime = Long.parseLong(envs.get(DSConstants.JARLOCATIONTIME));

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException("Cannot run distributed shell with no containers");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

        if(!cliParser.hasOption("input")) {
            throw new IllegalArgumentException("No input specified");
        }
        input = cliParser.getOptionValue("input");

        if(!cliParser.hasOption("output")) {
            throw new IllegalArgumentException("No output specified");
        }
        output = cliParser.getOptionValue("output");

        // Creating the Timeline Client
        timelineClient = TimelineClient.createTimelineClient();
        timelineClient.init(conf);
        timelineClient.start();

        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({ "unchecked" })
    public void run() throws YarnException, IOException, URISyntaxException {
        LOG.info("Starting ApplicationMaster");
        try {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(), DSEvent.DS_APP_ATTEMPT_START);
        } catch (Exception e) {
            LOG.error("App Attempt start event coud not be pulished for " + appAttemptID.toString(), e);
        }

        AMRMClientAsync.CallbackHandler amRmListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, amRmListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = new NMCallbackHandler(this);
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

        modifyRequiredResources(response);

        askContainers();

        try {
            publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(), DSEvent.DS_APP_ATTEMPT_END);
        } catch (Exception e) {
            LOG.error("App Attempt start event coud not be pulished for " + appAttemptID.toString(), e);
        }
    }

    private void askContainers() throws IOException {
        Path path = new Path(input);
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0L, fileStatus.getLen());
        LOG.info("Found " + fileBlockLocations.length + " blocks of file " + input);
        for (BlockLocation blockLocation: fileBlockLocations) {
            fileBlocks.addBlock(new Block(blockLocation));
            LOG.info("Asking container for block (" + blockLocation.getOffset() + "," + blockLocation.getLength() + ") of file " + input);
            ContainerRequest containerAsk = setupContainerAskForRM(blockLocation.getHosts());
            amRMClient.addContainerRequest(containerAsk);
            LOG.info("Asked container for block (" + blockLocation.getOffset() + "," + blockLocation.getLength() + ") of file " + input);
            numberContainers.incrementAndGet();
        }
    }

    private void modifyRequiredResources(RegisterApplicationMasterResponse response) {
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
        if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster. Using max value." + ", specified=" + containerMemory + ", max=" + maxMem);
            containerMemory = maxMem;
        }

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);
        if (containerVirtualCores > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster. Using max value." + ", specified=" + containerVirtualCores + ", max=" + maxVCores);
            containerVirtualCores = maxVCores;
        }
    }

    protected boolean finish() {
        while (!done) {
            try {
                LOG.info("Wait for completion...");
                Thread.sleep(1000);
            } catch (InterruptedException ex) {}
        }
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus = FinalApplicationStatus.SUCCEEDED;
        try {
            amRMClient.unregisterApplicationMaster(appStatus, null, null);
        } catch (YarnException | IOException ex) {
            LOG.error("Failed to unregister application", ex);
        }

        amRMClient.stop();

        return true;
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                if (containerStatus.getExitStatus() == ContainerExitStatus.SUCCESS) {
                    numberCompletedContainers.incrementAndGet();
                } else {
                    LOG.error("Container " + containerStatus.getContainerId() + " failed with status " + containerStatus.getExitStatus());
                }
                try {
                    publishContainerEndEvent(timelineClient, containerStatus);
                } catch (Exception e) {
                    LOG.error("Container start event could not be pulished for "
                            + containerStatus.getContainerId().toString(), e);
                }
            }
            if(numberContainers.get() == numberCompletedContainers.get()) {
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container allocated. Allocated containers number=" + allocatedContainers.size());

            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory="
                        + allocatedContainer.getResource().getMemory()
                        + ", containerResourceVirtualCores="
                        + allocatedContainer.getResource().getVirtualCores());

                allocateContainer(allocatedContainer);
            }
        }

        private void allocateContainer(Container allocatedContainer) {
            LOG.info("Set the environment for the application master");
            Map<String, String> env = new HashMap<>();

            String classPathEnv = getClasspath();

            env.put("CLASSPATH", classPathEnv);

            // Set the local resources
            Map<String, LocalResource> localResources = new HashMap<>();

            try {
                ResourcesUtils.addToLocalResources(localResources, new Path(jarPath), appMasterJarPath, jarPathLen, jarPathTime);
            } catch (IOException e) {
                LOG.error("Could not add to local resources file " + jarPath, e);
                throw new RuntimeException(e);
            }

            Block freeBlock = fileBlocks.getAndMarkInProgressFreeBlock(allocatedContainer.getNodeId().getHost());

            String command = Arrays.asList(
                    Environment.JAVA_HOME.$$() + "/bin/java",
                    com.epam.hadoop.hw2.container.Container.class.getName(),
                    param(CliConstants.INPUT, input),
                    param(CliConstants.OUTPUT, output),
                    param(CliConstants.OFFSET, freeBlock.getBlockLocation().getOffset()),
                    param(CliConstants.LENGTH, freeBlock.getBlockLocation().getLength()),
                    "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
                    "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
                    .stream()
                    .collect(Collectors.joining(" "));

            LOG.info("Container command = " + command);

            List<String> commands = new ArrayList<>();
            commands.add(command);

            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, env, commands, null, null, null);
            containerListener.addContainer(allocatedContainer.getId(), allocatedContainer);
            nmClientAsync.startContainerAsync(allocatedContainer, ctx);
        }

        private String getClasspath() {
            StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                    .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                    .append("./*");

            for (String c : conf.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(c.trim());
            }
            return classPathEnv.toString();
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {}

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            return 0.5F;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            amRMClient.stop();
        }
    }

    static class NMCallbackHandler
            implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<ContainerId, Container>();
        private final ApplicationMaster applicationMaster;

        public NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
            try {
                ApplicationMaster.publishContainerStartEvent(
                        applicationMaster.timelineClient, container);
            } catch (Exception e) {
                LOG.error("Container start event coud not be pulished for "
                        + container.getId().toString(), e);
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM(String[] nodes) {
        Priority pri = Priority.newInstance(requestPriority);

        Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);

        ContainerRequest request = new ContainerRequest(capability, nodes, null, pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

    private boolean fileExist(String filePath) {
        return new File(filePath).exists();
    }

    private String readContent(String filePath) throws IOException {
        DataInputStream ds = null;
        try {
            ds = new DataInputStream(new FileInputStream(filePath));
            return ds.readUTF();
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(ds);
        }
    }

    private static void publishContainerStartEvent(TimelineClient timelineClient,
                                                   Container container) throws IOException, YarnException {
        TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.addPrimaryFilter("user",
                UserGroupInformation.getCurrentUser().getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_START.toString());
        event.addEventInfo("Node", container.getNodeId().toString());
        event.addEventInfo("Resources", container.getResource().toString());
        entity.addEvent(event);

        timelineClient.putEntities(entity);
    }

    private static void publishContainerEndEvent(TimelineClient timelineClient,
                                                 ContainerStatus container) throws IOException, YarnException {
        TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getContainerId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.addPrimaryFilter("user", UserGroupInformation.getCurrentUser().getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_END.toString());
        event.addEventInfo("State", container.getState().name());
        event.addEventInfo("Exit Status", container.getExitStatus());
        entity.addEvent(event);

        timelineClient.putEntities(entity);
    }

    private static void publishApplicationAttemptEvent(
            TimelineClient timelineClient, String appAttemptId, DSEvent appEvent)
            throws IOException, YarnException {
        TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(appAttemptId);
        entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
        entity.addPrimaryFilter("user", UserGroupInformation.getCurrentUser().getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setEventType(appEvent.toString());
        event.setTimestamp(System.currentTimeMillis());
        entity.addEvent(event);

        timelineClient.putEntities(entity);
    }
}
