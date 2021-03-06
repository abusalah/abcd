/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
//import org.apache.hadoop.mapreduce.MRJobConfig;
//import org.apache.hadoop.mapreduce.MRJobConfig;
//import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler.PingChecker;
//import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler.ThreadedEchoServer4;
//import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler.clientThread;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
 *
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService implements Recoverable {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(ResourceManager.class);
  private static long clusterTimeStamp = System.currentTimeMillis();

  /**
   * "Always On" services. Services that need to run always irrespective of
   * the HA state of the RM.
   */
  @VisibleForTesting
  protected RMContextImpl rmContext;
  private Dispatcher rmDispatcher;
  @VisibleForTesting
  protected AdminService adminService;

  /**
   * "Active" services. Services that need to run only on the Active RM.
   * These services are managed (initialized, started, stopped) by the
   * {@link CompositeService} RMActiveServices.
   *
   * RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
   * in Active state.
   */
  protected RMActiveServices activeServices;
  protected RMSecretManagerService rmSecretManagerService;

  protected ResourceScheduler scheduler;
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  protected NodesListManager nodesListManager;
  protected RMAppManager rmAppManager;
  protected ApplicationACLsManager applicationACLsManager;
  protected QueueACLsManager queueACLsManager;
  private WebApp webApp;
  private AppReportFetcher fetcher = null;
  protected ResourceTrackerService resourceTracker;

  private String webAppAddress;
  private ConfigurationProvider configurationProvider = null;
  /** End of Active services */

  private Configuration conf;
  
  //public static int numOfMachines=0;
  
  //public static Configuration myConf;
  
//  private static Long[] replicasHashes; //= new Long[MRJobConfig.NUM_REDUCES];
//  private static int[] replicasHashes_set;
//  private static int local_BFT_flag=0;
//  private static int local_NUM_REPLICAS = 0;
//  
//  public static Thread ThreadedEchoServer4;
//  public static boolean stopFlag=false;
//  public static Socket clientSocket = null;
//  public static ServerSocket serverSocket = null;

  

  private UserGroupInformation rmLoginUGI;
  
  public ResourceManager() {
    super("ResourceManager");
  }

  public RMContext getRMContext() {
    return this.rmContext;
  }

  public static long getClusterTimeStamp() {
    return clusterTimeStamp;
  }

  @VisibleForTesting
  protected static void setClusterTimeStamp(long timestamp) {
    clusterTimeStamp = timestamp;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    this.rmContext = new RMContextImpl();

    this.configurationProvider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    this.configurationProvider.init(this.conf);
    rmContext.setConfigurationProvider(configurationProvider);

    // load core-site.xml
    InputStream coreSiteXMLInputStream =
        this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
    if (coreSiteXMLInputStream != null) {
      this.conf.addResource(coreSiteXMLInputStream);
    }

    // Do refreshUserToGroupsMappings with loaded core-site.xml
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(this.conf)
        .refresh();

    // Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
    ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);

    // load yarn-site.xml
    InputStream yarnSiteXMLInputStream =
        this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    if (yarnSiteXMLInputStream != null) {
      this.conf.addResource(yarnSiteXMLInputStream);
    }

    validateConfigs(this.conf);

    // register the handlers for all AlwaysOn services using setupDispatcher().
    rmDispatcher = setupDispatcher();
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);

    adminService = createAdminService();
    addService(adminService);
    rmContext.setRMAdminService(adminService);

    this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
    if (this.rmContext.isHAEnabled()) {
      HAUtil.verifyAndSetConfiguration(this.conf);
    }
    createAndInitActiveServices();

    webAppAddress = WebAppUtils.getRMWebAppURLWithoutScheme(this.conf);

    this.rmLoginUGI = UserGroupInformation.getCurrentUser();

    super.serviceInit(this.conf);
  }
  
  protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    return new QueueACLsManager(scheduler, conf);
  }

  @VisibleForTesting
  protected void setRMStateStore(RMStateStore rmStore) {
    rmStore.setRMDispatcher(rmDispatcher);
    rmContext.setStateStore(rmStore);
  }

  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    return new SchedulerEventDispatcher(this.scheduler);
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  protected ResourceScheduler createScheduler() {
    String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
        YarnConfiguration.DEFAULT_RM_SCHEDULER);
    LOG.info("Using Scheduler: " + schedulerClassName);
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
        return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
            this.conf);
      } else {
        throw new YarnRuntimeException("Class: " + schedulerClassName
            + " not instance of " + ResourceScheduler.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Scheduler: "
          + schedulerClassName, e);
    }
  }

  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(this.rmContext);
  }

  private NMLivelinessMonitor createNMLivelinessMonitor() {
    return new NMLivelinessMonitor(this.rmContext
        .getDispatcher());
  }

  protected AMLivelinessMonitor createAMLivelinessMonitor() {
    return new AMLivelinessMonitor(this.rmDispatcher);
  }
  
  protected DelegationTokenRenewer createDelegationTokenRenewer() {
    return new DelegationTokenRenewer();
  }

  protected RMAppManager createRMAppManager() {
    return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
      this.applicationACLsManager, this.conf);
  }

  protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
    return new RMApplicationHistoryWriter();
  }

  // sanity check for configurations
  protected static void validateConfigs(Configuration conf) {
    // validate max-attempts
    int globalMaxAppAttempts =
        conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    if (globalMaxAppAttempts <= 0) {
      throw new YarnRuntimeException("Invalid global max attempts configuration"
          + ", " + YarnConfiguration.RM_AM_MAX_ATTEMPTS
          + "=" + globalMaxAppAttempts + ", it should be a positive integer.");
    }

    // validate expireIntvl >= heartbeatIntvl
    long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    long heartbeatIntvl =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (expireIntvl < heartbeatIntvl) {
      throw new YarnRuntimeException("Nodemanager expiry interval should be no"
          + " less than heartbeat interval, "
          + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl
          + ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "="
          + heartbeatIntvl);
    }
  }

  /**
   * RMActiveServices handles all the Active services in the RM.
   */
  @Private
  class RMActiveServices extends CompositeService {

    private DelegationTokenRenewer delegationTokenRenewer;
    private EventHandler<SchedulerEvent> schedulerDispatcher;
    private ApplicationMasterLauncher applicationMasterLauncher;
    private ContainerAllocationExpirer containerAllocationExpirer;

    private boolean recoveryEnabled;

    RMActiveServices() {
      super("RMActiveServices");
    }

    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

      rmSecretManagerService = createRMSecretManagerService();
      addService(rmSecretManagerService);

      containerAllocationExpirer = new ContainerAllocationExpirer(rmDispatcher);
      addService(containerAllocationExpirer);
      rmContext.setContainerAllocationExpirer(containerAllocationExpirer);

      AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
      addService(amLivelinessMonitor);
      rmContext.setAMLivelinessMonitor(amLivelinessMonitor);

      AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
      addService(amFinishingMonitor);
      rmContext.setAMFinishingMonitor(amFinishingMonitor);

      boolean isRecoveryEnabled = conf.getBoolean(
          YarnConfiguration.RECOVERY_ENABLED,
          YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

      RMStateStore rmStore = null;
      if(isRecoveryEnabled) {
        recoveryEnabled = true;
        rmStore =  RMStateStoreFactory.getStore(conf);
      } else {
        recoveryEnabled = false;
        rmStore = new NullRMStateStore();
      }

      try {
        rmStore.init(conf);
        rmStore.setRMDispatcher(rmDispatcher);
      } catch (Exception e) {
        // the Exception from stateStore.init() needs to be handled for
        // HA and we need to give up master status if we got fenced
        LOG.error("Failed to init state store", e);
        throw e;
      }
      rmContext.setStateStore(rmStore);

      if (UserGroupInformation.isSecurityEnabled()) {
        delegationTokenRenewer = createDelegationTokenRenewer();
        rmContext.setDelegationTokenRenewer(delegationTokenRenewer);
      }

      RMApplicationHistoryWriter rmApplicationHistoryWriter =
          createRMApplicationHistoryWriter();
      addService(rmApplicationHistoryWriter);
      rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

      // Register event handler for NodesListManager
      nodesListManager = new NodesListManager(rmContext);
      rmDispatcher.register(NodesListManagerEventType.class, nodesListManager);
      addService(nodesListManager);
      rmContext.setNodesListManager(nodesListManager);
      
      //numOfMachines=nodesListManager.getHostsReader().getHosts().size();

      // Initialize the scheduler
      scheduler = createScheduler();
      rmContext.setScheduler(scheduler);

      schedulerDispatcher = createSchedulerEventDispatcher();
      addIfService(schedulerDispatcher);
      rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);

      // Register event handler for RmAppEvents
      rmDispatcher.register(RMAppEventType.class,
          new ApplicationEventDispatcher(rmContext));

      // Register event handler for RmAppAttemptEvents
      rmDispatcher.register(RMAppAttemptEventType.class,
          new ApplicationAttemptEventDispatcher(rmContext));

      // Register event handler for RmNodes
      rmDispatcher.register(
          RMNodeEventType.class, new NodeEventDispatcher(rmContext));

      nmLivelinessMonitor = createNMLivelinessMonitor();
      addService(nmLivelinessMonitor);

      resourceTracker = createResourceTrackerService();
      addService(resourceTracker);
      rmContext.setResourceTrackerService(resourceTracker);

      DefaultMetricsSystem.initialize("ResourceManager");
      JvmMetrics.initSingleton("ResourceManager", null);

      try {
        scheduler.reinitialize(conf, rmContext);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to initialize scheduler", ioe);
      }

      // creating monitors that handle preemption
      createPolicyMonitors();

      masterService = createApplicationMasterService();
      addService(masterService) ;
      rmContext.setApplicationMasterService(masterService);

      applicationACLsManager = new ApplicationACLsManager(conf);

      queueACLsManager = createQueueACLsManager(scheduler, conf);

      rmAppManager = createRMAppManager();
      // Register event handler for RMAppManagerEvents
      rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);

      clientRM = createClientRMService();
      rmContext.setClientRMService(clientRM);
      addService(clientRM);
      rmContext.setClientRMService(clientRM);

      applicationMasterLauncher = createAMLauncher();
      rmDispatcher.register(AMLauncherEventType.class,
          applicationMasterLauncher);

      addService(applicationMasterLauncher);
      if (UserGroupInformation.isSecurityEnabled()) {
        addService(delegationTokenRenewer);
        delegationTokenRenewer.setRMContext(rmContext);
      }

      new RMNMInfo(rmContext, scheduler);

      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      RMStateStore rmStore = rmContext.getStateStore();
      // The state store needs to start irrespective of recoveryEnabled as apps
      // need events to move to further states.
      rmStore.start();

      if(recoveryEnabled) {
        try {
          rmStore.checkVersion();
          RMState state = rmStore.loadState();
          recover(state);
        } catch (Exception e) {
          // the Exception from loadState() needs to be handled for
          // HA and we need to give up master status if we got fenced
          LOG.error("Failed to load/recover state", e);
          throw e;
        }
      }

      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {

      DefaultMetricsSystem.shutdown();

      if (rmContext != null) {
        RMStateStore store = rmContext.getStateStore();
        try {
          store.close();
        } catch (Exception e) {
          LOG.error("Error closing store.", e);
        }
      }

      super.serviceStop();
    }

    protected void createPolicyMonitors() {
      if (scheduler instanceof PreemptableResourceScheduler
          && conf.getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS)) {
        LOG.info("Loading policy monitors");
        List<SchedulingEditPolicy> policies = conf.getInstances(
            YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
            SchedulingEditPolicy.class);
        if (policies.size() > 0) {
          rmDispatcher.register(ContainerPreemptEventType.class,
              new RMContainerPreemptEventDispatcher(
                  (PreemptableResourceScheduler) scheduler));
          for (SchedulingEditPolicy policy : policies) {
            LOG.info("LOADING SchedulingEditPolicy:" + policy.getPolicyName());
            policy.init(conf, rmContext.getDispatcher().getEventHandler(),
                (PreemptableResourceScheduler) scheduler);
            // periodically check whether we need to take action to guarantee
            // constraints
            SchedulingMonitor mon = new SchedulingMonitor(policy);
            addService(mon);
          }
        } else {
          LOG.warn("Policy monitors configured (" +
              YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS +
              ") but none specified (" +
              YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES + ")");
        }
      }
    }
  }

    
    
//    //bft_______new code________________________________________________________________________________________________________
//    
//    
//    //static clientThread t[] = new clientThread[10];
//    public static ArrayList<clientThread> client_Threads_List = new ArrayList<clientThread>();  
//    
//    public static class ThreadedEchoServer4 implements Runnable {
//  	  
//  	  public void run(){
//  		  System.out.println("inside run() inside ThreadedEchoServer4 class");
//  			try {
//  				serverSocket = new ServerSocket(2222);
//  			} catch (IOException e) {
//  				System.out.println("\n\n\n\nserverSocket Exception\n\n\n");
//  				System.out.println(e);
//  			}
//
//  			while (!Thread.currentThread().isInterrupted()) {
//  				try {
//  		  		  System.out.println("inside try inside while (true) inside ThreadedEchoServer4 class");
//  					clientSocket = serverSocket.accept();
//  					System.out.println("inside try inside while (true) inside ThreadedEchoServer4 class AFTER clientSocket = serverSocket.accept();");
//  					//for (int i = 0; i <= 9; i++) 
//  					{
//  						//if (t[i] == null) 
//  						{
//  							clientThread tt = new clientThread(clientSocket, client_Threads_List);
//  							client_Threads_List.add(tt);
//  							tt.start();
//  							//t[i]=tt;//(t[i] = new clientThread(clientSocket, t)).start();
//  							//t[i].start();
//  							//break;
//  						}
//  					}
//  					//if(stopFlag==true)break;
//  				} catch (IOException e) {
//  					System.out.println(e);
//  				}
//  			}
////  			try {
////  				System.out.println("ENTERED before serverSocket.close() ");
////				serverSocket.close();
////				System.out.println("ENTERED after serverSocket.close() ");
////			} catch (IOException e) {
////				// TODO Auto-generated catch block
////				e.printStackTrace();
////			}
////  			System.out.println("END OF interruption of ThreadedEchoServer4 thread ");
//  		  
//  		  
//  	  }
//  	  
//    }
//    
//    
//    
//      
//    public static class clientThread extends Thread {
//
//  		DataInputStream is = null;
//  		PrintStream os = null;
//  		Socket clientSocket = null;
//  		clientThread t[];
//  		ArrayList<clientThread> client_Threads_List;
//  		
//  		 int receivedReducerNumber=0;
//         String receivedTaskAttemptID ="";
//         long receivedHash= 0;
//         Integer unreplicatedReducerNumber=null;
//         boolean firstandsecond,thirdandforth,allofthem;
//         
//
//  		/*//old constructor
//  		public clientThread(Socket clientSocket, clientThread[] t) {
//  			this.clientSocket = clientSocket;
//  			this.t = t;
//  		}
//  		 */
//  		public clientThread(Socket clientSocket, ArrayList<clientThread> client_Threads_List) {
//  			this.clientSocket = clientSocket;
//  			this.client_Threads_List = client_Threads_List;
//  		}
//  		
//  		public void run() {
//  			String lineReceived;
//  			String receivedOK;
//  			int ii =0;
//  			System.out.println("Inside run() inside clientThread class");
//  			try {
//  				is = new DataInputStream(clientSocket.getInputStream());
//  				os = new PrintStream(clientSocket.getOutputStream());
//  				System.out.println("Inside try inside run() inside clientThread class");
//  				
//  				while (true) {//(!stopped && !Thread.currentThread().isInterrupted())
//  					lineReceived = is.readLine();
//  					System.out.println("lineReceived inside ResourceManager = "+lineReceived);//NOTE the difference between os and System.out 
//  					
//  					
//  					receivedReducerNumber = Integer.parseInt(lineReceived.split(" ")[0]);
//  	                receivedTaskAttemptID = lineReceived.split(" ")[1];
//  	                receivedHash = Long.parseLong(lineReceived.split(" ")[2]);
//  	                unreplicatedReducerNumber = (int) Math.floor(receivedReducerNumber/local_NUM_REPLICAS); 
//  	                replicasHashes[receivedReducerNumber]=receivedHash;
//  	                replicasHashes_set[unreplicatedReducerNumber]+=1;
//  	                
//  	                System.out.println("---------------------------------------------------------------------------");
//  	                System.out.println("receivedReducerNumber = "+receivedReducerNumber+
//  	                		"receivedTaskAttemptID = " + receivedTaskAttemptID +
//  	                		"receivedHash = " + receivedHash +
//  	                		"unreplicatedReducerNumber = "+unreplicatedReducerNumber
//  	                		);
//  	                
//  	                
//  	                  for(int i =0;i<replicasHashes.length;i++)
//  	                  {
//  	               	   System.out.println("replicasHashes i = "+i+" is "+replicasHashes[i]);
//  	                  }
//  	                  for(int i =0;i<replicasHashes_set.length;i++)
//  	                  {
//  	               	   System.out.println("replicasHashes_set i = "+i+" is "+replicasHashes_set[i]);
//  	                  }
//  	                System.out.println("---------------------------------------------------------------------------");
//  	                  
//  	                if(replicasHashes_set[unreplicatedReducerNumber]==local_NUM_REPLICAS)//TODO make >=local_NUM_REPLICAS in case it is restarted from HeartBeats
//  	                {
//  	                	for(int i=0;i<local_NUM_REPLICAS-1;i++)//NOTE ... that it is from 0 to <local_NUM_REPLICAS-1 ... which means 0 to =local_NUM_REPLICAS-2  
//  	                	{
//  	                		System.out.println("local_NUM_REPLICAS = "+local_NUM_REPLICAS);
//  	                		System.out.println("replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i] = "+replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i]);
//  	                		System.out.println("replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1] = "+replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1]);
//  	                		if(replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i].equals(replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1]))//==replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1])
//  	                		{
//  	                			System.out.println("ENTERED allofthem=true;");
//  	                			allofthem=true;
//  	                		}else {
//  	                			System.out.println("ENTERED allofthem=false;");
//  	                			allofthem=false;//CAREFUL ... if you didn't add break here, allofthem can become true in the next round and gives a wrong allofthem=true (I.L)
//  	                			break;//TODO ... need to add what to do when the replicas don't match  
//  	                		}
//  	                	}
//  	             	   //firstandsecond = (replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+0] == replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+1]);
//  	             	   //thirdandforth = (replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+2] == replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+3]);
//  	             	   //allofthem = (firstandsecond == thirdandforth);
//  	             	   if (allofthem==true)
//  	             	   {
//  	             		   System.out.println("ALL CORRECT FOR REDUCER "+unreplicatedReducerNumber);
//  	             		for(clientThread x:client_Threads_List)
//  	   					{
//  	             			System.out.println("client_Threads_List.size() = "+client_Threads_List.size());
//  	   						x.os.println(unreplicatedReducerNumber);//unreplicatedReducerNumber);//x.os.println("XXXX");	   						
//  	   					}
//  	             		
//  	             		/*
//  	             		ii=0;
//  	             		for(clientThread x:client_Threads_List)
//  	   					{	
//  	   						receivedOK = is.readLine();
//  	   						if(receivedOK=="ok")
//  	   						{
//  	   							System.out.println("RECEIVED OK FROM ii = "+ii);
//  	   							System.out.println("BEFORE client_Threads_List.size() = "+client_Threads_List.size());
//  	   							client_Threads_List.remove(ii);
//  	   							System.out.println("AFTER client_Threads_List.size() = "+client_Threads_List.size());
//  	   							}
//  	   						ii++;
//  	   					}
//  	   					*/
//  	             		System.out.println("=========AFTER x.os.println(unreplicatedReducerNumber)=============	");
//  	             		
//  	             		
//  	             		 //capitalizedSentence = clientSentence.toUpperCase() + '\n';
//  	             		    //System.out.println("lineReceived = "+lineReceived);
//  		                    //out.writeBytes(unreplicatedReducerNumber + "\n\r");
//  		                    //out.flush();
//  		                	
//  	             		  
//  	             		   
//  	             	   }
//  	                }
//  	                if (lineReceived.startsWith("ww"))//TODO NEED TO HAVE A BETTER WAY TO CLOSE THE THREAD
//  						break;
//  					
//  				}
//  				
//  				is.close();
//  				os.close();
//  				clientSocket.close();
//  			} catch (IOException e) {
//  			}
//  			;
//  		}
//  	}
//
//
//
//    //bft_______end new code____________________________________________________________________________________________________
//
//    
    
    
    
    
    
    
    @Private
    public static class SchedulerEventDispatcher extends AbstractService
        implements EventHandler<SchedulerEvent> {

      private final ResourceScheduler scheduler;
      private final BlockingQueue<SchedulerEvent> eventQueue =
        new LinkedBlockingQueue<SchedulerEvent>();
      private final Thread eventProcessor;
      private volatile boolean stopped = false;
      private boolean shouldExitOnError = false;

      public SchedulerEventDispatcher(ResourceScheduler scheduler) {
        super(SchedulerEventDispatcher.class.getName());
        this.scheduler = scheduler;
        this.eventProcessor = new Thread(new EventProcessor());
        this.eventProcessor.setName("ResourceManager Event Processor");
      }

      @Override
      protected void serviceInit(Configuration conf) throws Exception {
        this.shouldExitOnError =
            conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
              Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
        super.serviceInit(conf);
      }

      @Override
      protected void serviceStart() throws Exception {
        this.eventProcessor.start();
        super.serviceStart();
      }
      

    
    
    
    
    
    
    
    private final class EventProcessor implements Runnable {
      @Override
      public void run() {
    	  SchedulerEvent event;
    	  

        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return; // TODO: Kill RM.
          }

          try {
//        	  System.out.println("\n\n\n\n------HERE THE SCHEDULER STARTS FROM THE RESOURCE MANAGER------scheduler.handle(event);-------"
//        	  		+ " INSIDE ResourceManager.java in org.apache.hadoop.yarn.server.resourcemanager package "
//        	  		+ " in hadoop-yarn-server-resourcemanager project ----------"
//        	  		+ " it takes an event from an eventQueue and it handles it. "
//        	  		+ " This event is a task(or a container) for example   \n\n\n\n");
        
        	  
            scheduler.handle(event);
          } catch (Throwable t) {
            // An error occurred, but we are shutting down anyway.
            // If it was an InterruptedException, the very act of 
            // shutdown could have caused it and is probably harmless.
            if (stopped) {
              LOG.warn("Exception during shutdown: ", t);
              break;
            }
            LOG.fatal("Error in handling event type " + event.getType()
                + " to the scheduler", t);
            if (shouldExitOnError
                && !ShutdownHookManager.get().isShutdownInProgress()) {
              LOG.info("Exiting, bbye..");
              System.exit(-1);
            }
          }
        }
      }
    }

    @Override
    protected void serviceStop() throws Exception {
      this.stopped = true;
      this.eventProcessor.interrupt();
      //ThreadedEchoServer4.interrupt();
      //stopFlag=true;
      //client_Threads_List.clear();
//      serverSocket.close();
//      for(clientThread x:client_Threads_List)
//	  {
//			x.clientSocket.close();
//			x.is.close();
// 			x.interrupt();
// 			x.os.close();
//      }
 		
      try {
        this.eventProcessor.join();
      } catch (InterruptedException e) {
        throw new YarnRuntimeException(e);
      }
      super.serviceStop();
    }

    @Override
    public void handle(SchedulerEvent event) {
      try {
        int qSize = eventQueue.size();
        if (qSize !=0 && qSize %1000 == 0) {
          LOG.info("Size of scheduler event-queue is " + qSize);
        }
        int remCapacity = eventQueue.remainingCapacity();
        if (remCapacity < 1000) {
          LOG.info("Very low remaining capacity on scheduler event queue: "
              + remCapacity);
        }
        this.eventQueue.put(event);
      } catch (InterruptedException e) {
        LOG.info("Interrupted. Trying to exit gracefully.");
      }
    }
  }

  @Private
  public static class RMFatalEventDispatcher
      implements EventHandler<RMFatalEvent> {
    private final RMContext rmContext;
    private final ResourceManager rm;

    public RMFatalEventDispatcher(
        RMContext rmContext, ResourceManager resourceManager) {
      this.rmContext = rmContext;
      this.rm = resourceManager;
    }

    @Override
    public void handle(RMFatalEvent event) {
      LOG.fatal("Received a " + RMFatalEvent.class.getName() + " of type " +
          event.getType().name() + ". Cause:\n" + event.getCause());

      if (event.getType() == RMFatalEventType.STATE_STORE_FENCED) {
        LOG.info("RMStateStore has been fenced");
        if (rmContext.isHAEnabled()) {
          try {
            // Transition to standby and reinit active services
            LOG.info("Transitioning RM to Standby mode");
            rm.transitionToStandby(true);
            return;
          } catch (Exception e) {
            LOG.fatal("Failed to transition RM to Standby mode.");
          }
        }
      }

      ExitUtil.terminate(1, event.getCause());
    }
  }

  @Private
  public static final class ApplicationEventDispatcher implements
      EventHandler<RMAppEvent> {

    private final RMContext rmContext;

    public ApplicationEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppEvent event) {
      ApplicationId appID = event.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appID);
      if (rmApp != null) {
        try {
          rmApp.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appID, t);
        }
      }
    }
  }

  @Private
  public static final class
    RMContainerPreemptEventDispatcher
      implements EventHandler<ContainerPreemptEvent> {

    private final PreemptableResourceScheduler scheduler;

    public RMContainerPreemptEventDispatcher(
        PreemptableResourceScheduler scheduler) {
      this.scheduler = scheduler;
    }

    @Override
    public void handle(ContainerPreemptEvent event) {
      ApplicationAttemptId aid = event.getAppId();
      RMContainer container = event.getContainer();
      switch (event.getType()) {
      case DROP_RESERVATION:
        scheduler.dropContainerReservation(container);
        break;
      case PREEMPT_CONTAINER:
        scheduler.preemptContainer(aid, container);
        break;
      case KILL_CONTAINER:
        scheduler.killContainer(container);
        break;
      }
    }
  }

  @Private
  public static final class ApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

    private final RMContext rmContext;

    public ApplicationAttemptEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppAttemptEvent event) {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      ApplicationId appAttemptId = appAttemptID.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appAttemptId);
      if (rmApp != null) {
        RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptID);
        if (rmAppAttempt != null) {
          try {
            rmAppAttempt.handle(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " for applicationAttempt " + appAttemptId, t);
          }
        }
      }
    }
  }

  @Private
  public static final class NodeEventDispatcher implements
      EventHandler<RMNodeEvent> {

    private final RMContext rmContext;

    public NodeEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMNodeEvent event) {
      NodeId nodeId = event.getNodeId();
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      if (node != null) {
        try {
          ((EventHandler<RMNodeEvent>) node).handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for node " + nodeId, t);
        }
      }
    }
  }
  
  protected void startWepApp() {
    Builder<ApplicationMasterService> builder = 
        WebApps
            .$for("cluster", ApplicationMasterService.class, masterService,
                "ws")
            .with(conf)
            .withHttpSpnegoPrincipalKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
            .withHttpSpnegoKeytabKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
            .at(webAppAddress);
    String proxyHostAndPort = WebAppUtils.getProxyHostAndPort(conf);
    if(WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
        equals(proxyHostAndPort)) {
      if (HAUtil.isHAEnabled(conf)) {
        fetcher = new AppReportFetcher(conf);
      } else {
        fetcher = new AppReportFetcher(conf, getClientRMService());
      }
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);

    }
    webApp = builder.start(new RMWebApp(this));
  }

  /**
   * Helper method to create and init {@link #activeServices}. This creates an
   * instance of {@link RMActiveServices} and initializes it.
   * @throws Exception
   */
  void createAndInitActiveServices() throws Exception {
    activeServices = new RMActiveServices();
    activeServices.init(conf);
  }

  /**
   * Helper method to start {@link #activeServices}.
   * @throws Exception
   */
  void startActiveServices() throws Exception {
    if (activeServices != null) {
      clusterTimeStamp = System.currentTimeMillis();
      activeServices.start();
    }
  }

  /**
   * Helper method to stop {@link #activeServices}.
   * @throws Exception
   */
  void stopActiveServices() throws Exception {
    if (activeServices != null) {
      activeServices.stop();
      activeServices = null;
      rmContext.getRMNodes().clear();
      rmContext.getInactiveRMNodes().clear();
      rmContext.getRMApps().clear();
      ClusterMetrics.destroy();
      QueueMetrics.clearQueueMetrics();
    }
    //ThreadedEchoServer4.interrupt();
    //stopFlag=true;
    //client_Threads_List.clear();

//    serverSocket.close();
//    for(clientThread x:client_Threads_List)
//	  {
//			x.clientSocket.close();
//			x.is.close();
//			x.interrupt();
//			x.os.close();
//    }
		
  }

  @VisibleForTesting
  protected boolean areActiveServicesRunning() {
    return activeServices != null && activeServices.isInState(STATE.STARTED);
  }

  synchronized void transitionToActive() throws Exception {
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }

    LOG.info("Transitioning to active state");

    // use rmLoginUGI to startActiveServices.
    // in non-secure model, rmLoginUGI will be current UGI
    // in secure model, rmLoginUGI will be LoginUser UGI
    this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        startActiveServices();
        return null;
      }
    });

    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
    LOG.info("Transitioned to active state");
  }

  synchronized void transitionToStandby(boolean initialize)
      throws Exception {
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.STANDBY) {
      LOG.info("Already in standby state");
      return;
    }

    LOG.info("Transitioning to standby state");
    if (rmContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.ACTIVE) {
      stopActiveServices();
      if (initialize) {
        resetDispatcher();
        createAndInitActiveServices();
      }
    }
    rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);
    LOG.info("Transitioned to standby state");
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }

    if (this.rmContext.isHAEnabled()) {
      transitionToStandby(true);
    } else {
      transitionToActive();
    }

    startWepApp();
    if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      int port = webApp.port();
      WebAppUtils.setRMWebAppPort(conf, port);
    }
    super.serviceStart();
  }
  
  protected void doSecureLogin() throws IOException {
	InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(this.conf, YarnConfiguration.RM_KEYTAB,
        YarnConfiguration.RM_PRINCIPAL, socAddr.getHostName());

    // if security is enable, set rmLoginUGI as UGI of loginUser
    if (UserGroupInformation.isSecurityEnabled()) {
      this.rmLoginUGI = UserGroupInformation.getLoginUser();
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    if (fetcher != null) {
      fetcher.stop();
    }
    if (configurationProvider != null) {
      configurationProvider.close();
    }
    super.serviceStop();
    transitionToStandby(false);
    rmContext.setHAServiceState(HAServiceState.STOPPING);
  }
  
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(this.rmContext, this.nodesListManager,
        this.nmLivelinessMonitor,
        this.rmContext.getContainerTokenSecretManager(),
        this.rmContext.getNMTokenSecretManager());
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
        this.applicationACLsManager, this.queueACLsManager,
        this.rmContext.getRMDelegationTokenSecretManager());
  }

  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(this.rmContext, scheduler);
  }

  protected AdminService createAdminService() {
    return new AdminService(this, rmContext);
  }

  protected RMSecretManagerService createRMSecretManagerService() {
    return new RMSecretManagerService(conf, rmContext);
  }

  @Private
  public ClientRMService getClientRMService() {
    return this.clientRM;
  }
  
  /**
   * return the scheduler.
   * @return the scheduler for the Resource Manager.
   */
  @Private
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }

  /**
   * return the resource tracking component.
   * @return the resource tracking component.
   */
  @Private
  public ResourceTrackerService getResourceTrackerService() {
    return this.resourceTracker;
  }

  @Private
  public ApplicationMasterService getApplicationMasterService() {
    return this.masterService;
  }

  @Private
  public ApplicationACLsManager getApplicationACLsManager() {
    return this.applicationACLsManager;
  }

  @Private
  public QueueACLsManager getQueueACLsManager() {
    return this.queueACLsManager;
  }

  @Private
  WebApp getWebapp() {
    return this.webApp;
  }

  @Override
  public void recover(RMState state) throws Exception {
    // recover RMdelegationTokenSecretManager
    rmContext.getRMDelegationTokenSecretManager().recover(state);

    // recover applications
    rmAppManager.recover(state);
  }

  public static void main(String argv[]) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
    try {
      Configuration conf = new YarnConfiguration();
      ResourceManager resourceManager = new ResourceManager();
      ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(resourceManager),
        SHUTDOWN_HOOK_PRIORITY);
      resourceManager.init(conf);
      resourceManager.start();
      
//      local_BFT_flag =conf.getInt(MRJobConfig.BFT_FLAG, 1);
//      local_NUM_REPLICAS =conf.getInt(MRJobConfig.NUM_REPLICAS,4);
//      replicasHashes = new Long[conf.getInt(MRJobConfig.NUM_REDUCES, 1)];
//      replicasHashes_set = new int[conf.getInt(MRJobConfig.NUM_REDUCES, 1)/local_NUM_REPLICAS];    
      
	  
//	    local_BFT_flag =conf.getInt("mapred.job.bft", 1);
//	    local_NUM_REPLICAS =conf.getInt("mapred.job.numreplicas",4);
//	    replicasHashes = new Long[conf.getInt("mapreduce.job.reduces", 1)];
//	    replicasHashes_set = new int[conf.getInt("mapreduce.job.reduces", 1)/local_NUM_REPLICAS]; 
//	    
//	    System.out.println("local_BFT_flag = "+local_BFT_flag);
//	    System.out.println("local_NUM_REPLICAS = "+local_NUM_REPLICAS);
//	    System.out.println("conf.getInt(\"mapreduce.job.reduces\", 1) = "+conf.getInt("mapreduce.job.reduces", 1));
//	    
//
//	  
//	    if(local_BFT_flag==3)//case 3 start the verification thread .... TODO NEED TO ADD CASE 2
//	    {
//		    ThreadedEchoServer4=new Thread(new ThreadedEchoServer4());
//		    ThreadedEchoServer4.setName("ThreadedEchoServer4 Thread");
//		    ThreadedEchoServer4.start();
//	    }

    } catch (Throwable t) {
      LOG.fatal("Error starting ResourceManager", t);
      System.exit(-1);
    }
  }

  /**
   * Register the handlers for alwaysOn services
   */
  private Dispatcher setupDispatcher() {
    Dispatcher dispatcher = createDispatcher();
    dispatcher.register(RMFatalEventType.class,
        new ResourceManager.RMFatalEventDispatcher(this.rmContext, this));
    return dispatcher;
  }

  private void resetDispatcher() {
    Dispatcher dispatcher = setupDispatcher();
    ((Service)dispatcher).init(this.conf);
    ((Service)dispatcher).start();
    removeService((Service)rmDispatcher);
    rmDispatcher = dispatcher;
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);
  }

  /**
   * Retrieve RM bind address from configuration
   * 
   * @param conf
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
  }
}
