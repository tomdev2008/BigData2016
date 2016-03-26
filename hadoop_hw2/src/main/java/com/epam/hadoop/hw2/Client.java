package com.epam.hadoop.hw2;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.epam.hadoop.hw2.appmaster.ApplicationMaster;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Client for Distributed Shell application submission to YARN.
 *
 * <p> The distributed shell client allows an application master to be launched that in turn would run
 * the provided shell command on a set of containers. </p>
 *
 * <p>This client is meant to act as an example on how to write yarn-based applications. </p>
 *
 * <p> To submit an application, a client first needs to connect to the <code>ResourceManager</code>
 * aka ApplicationsManager or ASM via the {@link ApplicationClientProtocol}. The {@link ApplicationClientProtocol}
 * provides a way for the client to get access to cluster information and to request for a
 * new {@link ApplicationId}. <p>
 *
 * <p> For the actual job submission, the client first has to create an {@link ApplicationSubmissionContext}.
 * The {@link ApplicationSubmissionContext} defines the application details such as {@link ApplicationId}
 * and application name, the priority assigned to the application and the queue
 * to which this application needs to be assigned. In addition to this, the {@link ApplicationSubmissionContext}
 * also defines the {@link ContainerLaunchContext} which describes the <code>Container</code> with which
 * the {@link ApplicationMaster} is launched. </p>
 *
 * <p> The {@link ContainerLaunchContext} in this scenario defines the resources to be allocated for the
 * {@link ApplicationMaster}'s container, the local resources (jars, configuration files) to be made available
 * and the environment to be set for the {@link ApplicationMaster} and the commands to be executed to run the
 * {@link ApplicationMaster}. <p>
 *
 * <p> Using the {@link ApplicationSubmissionContext}, the client submits the application to the
 * <code>ResourceManager</code> and then monitors the application by requesting the <code>ResourceManager</code>
 * for an {@link ApplicationReport} at regular time intervals. In case of the application taking too long, the client
 * kills the application by submitting a {@link KillApplicationRequest} to the <code>ResourceManager</code>. </p>
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

    private static final Log LOG = LogFactory.getLog(Client.class);

    // Configuration
    private Configuration conf;
    private YarnClient yarnClient;
    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "default";
    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 10;
    // Amt. of virtual core resource to request for to run the App Master
    private int amVCores = 1;

    // Application master jar file
    private String appMasterJar = "";
    // Main class to invoke application master
    private final String appMasterMainClass;

    // Shell Command Container priority
    private int shellCmdPriority = 0;

    // Amt of memory to request for container in which shell script will be executed
    private int containerMemory = 10;
    // Amt. of virtual cores to request for container in which shell script will be executed
    private int containerVirtualCores = 1;
    // No. of containers in which the shell script needs to be executed
    private int numContainers = 1;

    // flag to indicate whether to keep containers across application attempts.
    private boolean keepContainers = false;

    // Command line options
    private Options opts;

    private static final String appMasterJarPath = "AppMaster.jar";

    private String input = "";
    private String output = "";

    /**
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            Client client = new Client();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running CLient", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }

    public Client(Configuration conf) {
        this.conf = conf;
        this.appMasterMainClass = ApplicationMaster.class.getName();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - LinksProcessor");
        opts.addOption("jar", true, "Jar file containing the application master");
        opts.addOption("input", true, "Input file");
        opts.addOption("output", true, "Output file");
        opts.addOption("help", false, "Print usage");
    }

    /**
     */
    public Client() throws Exception  {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * Parse command line options
     * @param args Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        appName = cliParser.getOptionValue("appname", "LinksProcessor");

        if(!cliParser.hasOption("input")) {
            throw new IllegalArgumentException("No input specified");
        }
        input = cliParser.getOptionValue("input");

        if(!cliParser.hasOption("output")) {
            throw new IllegalArgumentException("No output specified");
        }
        output = cliParser.getOptionValue("output");

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }

        appMasterJar = cliParser.getOptionValue("jar");

        return true;
    }

    /**
     * Main run function for the client
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public boolean run() throws IOException, YarnException {
        LOG.info("Running Client");
        yarnClient.start();

        printClusterInfo();

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        modifyRequiredResources(appResponse);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers); //TODO
        appContext.setApplicationName(appName); //TODO

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        FileSystem fs = FileSystem.get(conf);
        FileInfo jarFileInfo = ResourcesUtils.copyAndAddToLocalResources(fs, appMasterJar, appMasterJarPath, appContext.getApplicationId().toString(), localResources, appName);

        Map<String, String> env = getEnvVars(jarFileInfo);
        StringBuilder classPathEnv = getClassPath();

        env.put("CLASSPATH", classPathEnv.toString());

        String command = Arrays.asList(Environment.JAVA_HOME.$$() + "/bin/java",
                "-Xmx" + amMemory + "m",
                appMasterMainClass,
                "--input " + input,
                "--output " + output,
                "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout",
                "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr")
                .stream().collect(Collectors.joining(" "));

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command);

        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);

        Priority pri = Priority.newInstance(amPriority);
        appContext.setPriority(pri);
        appContext.setQueue(amQueue);

        LOG.info("Submitting application to ASM");

        yarnClient.submitApplication(appContext);

        return monitorApplication(appContext.getApplicationId());

    }

    private StringBuilder getClassPath() {
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        return classPathEnv;
    }

    private Map<String, String> getEnvVars(FileInfo jarFileInfo) {
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();
        env.put(DSConstants.JARLOCATION, jarFileInfo.getPath().toString());
        env.put(DSConstants.JARLOCATIONLEN, jarFileInfo.getLength().toString());
        env.put(DSConstants.JARLOCATIONTIME, jarFileInfo.getModificationTime().toString());
        return env;
    }

    private void modifyRequiredResources(GetNewApplicationResponse appResponse) {
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value, specified=" + amMemory + ", max=" + maxMem);
            amMemory = maxMem;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);
        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. Using max value, specified=" + amVCores + ", max=" + maxVCores);
            amVCores = maxVCores;
        }
    }

    private void printClusterInfo() throws YarnException, IOException {
        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId()
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }
    }

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            }
            else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }
        }

    }

}

