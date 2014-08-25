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

package org.apache.hadoop.mapred;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenSelector;
//import org.apache.hadoop.yarn.server.resourcemanager.ThreadedEchoServer4;
//import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.clientThread;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class enables the current JobClient (0.22 hadoop) to run on YARN.
 */
@SuppressWarnings("unchecked")
public class YARNRunner implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private ResourceMgrDelegate resMgrDelegate;
  private ClientCache clientCache;
  private Configuration conf;
  private final FileContext defaultFileContext;
  
  
  private static Long[] replicasHashes; //= new Long[MRJobConfig.NUM_REDUCES];
  private static int[] replicasHashes_set;
  private static int local_BFT_flag=0;
  private static int local_NUM_REPLICAS = 0;
  
  public static Thread ThreadedEchoServer4;
  public static boolean stopFlag=false;
  public static Socket clientSocket = null;
  public static ServerSocket serverSocket = null;
  
  //bft_______new code________________________________________________________________________________________________________
  
  
  //static clientThread t[] = new clientThread[10];
  public static ArrayList<clientThread> client_Threads_List = new ArrayList<clientThread>();  
  
  public static class ThreadedEchoServer4 implements Runnable {
	  
	  public void run(){
		  System.out.println("inside run() inside ThreadedEchoServer4 class inside YARNRunner.java");
			try {
				serverSocket = new ServerSocket(2222);
			} catch (IOException e) {
				System.out.println("\n\n\n\nserverSocket Exception\n\n\n");
				System.out.println(e);
			}

			while (!Thread.currentThread().isInterrupted()) {
				try {
		  		  System.out.println("inside try inside while (true) inside ThreadedEchoServer4 class");
					clientSocket = serverSocket.accept();
					System.out.println("inside try inside while (true) inside ThreadedEchoServer4 class AFTER clientSocket = serverSocket.accept();");
					//for (int i = 0; i <= 9; i++) 
					{
						//if (t[i] == null) 
						{
							clientThread tt = new clientThread(clientSocket, client_Threads_List);
							client_Threads_List.add(tt);
							tt.start();
							//t[i]=tt;//(t[i] = new clientThread(clientSocket, t)).start();
							//t[i].start();
							//break;
						}
					}
					//if(stopFlag==true)break;
				} catch (IOException e) {
					System.out.println(e);
				}
			}
//			try {
//				System.out.println("ENTERED before serverSocket.close() ");
//				serverSocket.close();
//				System.out.println("ENTERED after serverSocket.close() ");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			System.out.println("END OF interruption of ThreadedEchoServer4 thread ");
		  
		  
	  }
	  
  }
  
  
  
    
  public static class clientThread extends Thread {

		DataInputStream is = null;
		PrintStream os = null;
		Socket clientSocket = null;
		clientThread t[];
		ArrayList<clientThread> client_Threads_List;
		
		 int receivedReducerNumber=0;
       String receivedTaskAttemptID ="";
       long receivedHash= 0;
       Integer unreplicatedReducerNumber=null;
       boolean firstandsecond,thirdandforth,allofthem;
       

		/*//old constructor
		public clientThread(Socket clientSocket, clientThread[] t) {
			this.clientSocket = clientSocket;
			this.t = t;
		}
		 */
		public clientThread(Socket clientSocket, ArrayList<clientThread> client_Threads_List) {
			this.clientSocket = clientSocket;
			this.client_Threads_List = client_Threads_List;
		}
		
		public void run() {
			String lineReceived;
			String receivedOK;
			int ii =0;
			System.out.println("Inside run() inside clientThread class");
			try {
				is = new DataInputStream(clientSocket.getInputStream());
				os = new PrintStream(clientSocket.getOutputStream());
				System.out.println("Inside try inside run() inside clientThread class");
				
				while (true) {//(!stopped && !Thread.currentThread().isInterrupted())
					lineReceived = is.readLine();//NOTE the difference between os and System.out
					//if(lineReceived!=null && !lineReceived.isEmpty())
					{
						System.out.println("lineReceived inside YARNRunner from reducer = "+lineReceived); 
						
						
						receivedReducerNumber = Integer.parseInt(lineReceived.split(" ")[0]);
		                receivedTaskAttemptID = lineReceived.split(" ")[1];
		                receivedHash = Long.parseLong(lineReceived.split(" ")[2]);
		                unreplicatedReducerNumber = (int) Math.floor(receivedReducerNumber/local_NUM_REPLICAS); 
		                replicasHashes[receivedReducerNumber]=receivedHash;
		                replicasHashes_set[unreplicatedReducerNumber]+=1;
		                
		                System.out.println("---------------------------------------------------------------------------");
		                System.out.println("receivedReducerNumber = "+receivedReducerNumber+
		                		"receivedTaskAttemptID = " + receivedTaskAttemptID +
		                		"receivedHash = " + receivedHash +
		                		"unreplicatedReducerNumber = "+unreplicatedReducerNumber
		                		);
					}
	                
	                
	                  for(int i =0;i<replicasHashes.length;i++)
	                  {
	               	   System.out.println("replicasHashes i = "+i+" is "+replicasHashes[i]);
	                  }
	                  for(int i =0;i<replicasHashes_set.length;i++)
	                  {
	               	   System.out.println("replicasHashes_set i = "+i+" is "+replicasHashes_set[i]);
	                  }
	                System.out.println("---------------------------------------------------------------------------");
	                  
	                if(replicasHashes_set[unreplicatedReducerNumber]==local_NUM_REPLICAS)//TODO make >=local_NUM_REPLICAS in case it is restarted from HeartBeats
	                {
	                	for(int i=0;i<local_NUM_REPLICAS-1;i++)//NOTE ... that it is from 0 to <local_NUM_REPLICAS-1 ... which means 0 to =local_NUM_REPLICAS-2  
	                	{
	                		System.out.println("local_NUM_REPLICAS = "+local_NUM_REPLICAS);
	                		System.out.println("replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i] = "+replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i]);
	                		System.out.println("replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1] = "+replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1]);
	                		if(replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i].equals(replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1]))//==replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1])
	                		{
	                			System.out.println("ENTERED allofthem=true;");
	                			allofthem=true;
	                		}else {
	                			System.out.println("ENTERED allofthem=false;");
	                			allofthem=false;//CAREFUL ... if you didn't add break here, allofthem can become true in the next round and gives a wrong allofthem=true (I.L)
	                			break;//TODO ... need to add what to do when the replicas don't match  
	                		}
	                	}
	             	   //firstandsecond = (replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+0] == replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+1]);
	             	   //thirdandforth = (replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+2] == replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+3]);
	             	   //allofthem = (firstandsecond == thirdandforth);
	             	   if (allofthem==true)
	             	   {
	             		   System.out.println("ALL CORRECT FOR REDUCER "+unreplicatedReducerNumber);
	             		for(clientThread x:client_Threads_List)
	   					{
	             			System.out.println("client_Threads_List.size() = "+client_Threads_List.size());
	   						x.os.println(unreplicatedReducerNumber);//unreplicatedReducerNumber);//x.os.println("XXXX");	   						
	   					}
	             		
	             		/*
	             		ii=0;
	             		for(clientThread x:client_Threads_List)
	   					{	
	   						receivedOK = is.readLine();
	   						if(receivedOK=="ok")
	   						{
	   							System.out.println("RECEIVED OK FROM ii = "+ii);
	   							System.out.println("BEFORE client_Threads_List.size() = "+client_Threads_List.size());
	   							client_Threads_List.remove(ii);
	   							System.out.println("AFTER client_Threads_List.size() = "+client_Threads_List.size());
	   							}
	   						ii++;
	   					}
	   					*/
	             		System.out.println("=========AFTER x.os.println(unreplicatedReducerNumber)=============	");
	             		
	             		
	             		 //capitalizedSentence = clientSentence.toUpperCase() + '\n';
	             		    //System.out.println("lineReceived = "+lineReceived);
		                    //out.writeBytes(unreplicatedReducerNumber + "\n\r");
		                    //out.flush();
		                	
	             		  
	             		   
	             	   }
	                }
	                if (lineReceived.startsWith("ww"))//TODO NEED TO HAVE A BETTER WAY TO CLOSE THE THREAD
						break;
					
				
				
				is.close();
				os.close();
				clientSocket.close();
				}
			} catch (IOException e) {
			}
			;
		}
	}



  //bft_______end new code____________________________________________________________________________________________________

  

  
  /**
   * Yarn runner incapsulates the client interface of
   * yarn
   * @param conf the configuration object for the client
   */
  public YARNRunner(Configuration conf) {
   this(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)));
  }

  /**
   * Similar to {@link #YARNRunner(Configuration)} but allowing injecting
   * {@link ResourceMgrDelegate}. Enables mocking and testing.
   * @param conf the configuration object for the client
   * @param resMgrDelegate the resourcemanager client handle.
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate) {
   this(conf, resMgrDelegate, new ClientCache(conf, resMgrDelegate));
  }

  /**
   * Similar to {@link YARNRunner#YARNRunner(Configuration, ResourceMgrDelegate)}
   * but allowing injecting {@link ClientCache}. Enable mocking and testing.
   * @param conf the configuration object
   * @param resMgrDelegate the resource manager delegate
   * @param clientCache the client cache object.
   */
  public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate,
      ClientCache clientCache) {
    this.conf = conf;
    try {
      this.resMgrDelegate = resMgrDelegate;
      this.clientCache = clientCache;
      this.defaultFileContext = FileContext.getFileContext(this.conf);
    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
  }
  
  @Private
  /**
   * Used for testing mostly.
   * @param resMgrDelegate the resource manager delegate to set to.
   */
  public void setResourceMgrDelegate(ResourceMgrDelegate resMgrDelegate) {
    this.resMgrDelegate = resMgrDelegate;
  }
  
  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Use Token.renew instead");
  }

  @Override
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getActiveTrackers();
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    return resMgrDelegate.getAllJobs();
  }

  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getBlacklistedTrackers();
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    return resMgrDelegate.getClusterMetrics();
  }

  @VisibleForTesting
  void addHistoryToken(Credentials ts) throws IOException, InterruptedException {
    /* check if we have a hsproxy, if not, no need */
    MRClientProtocol hsProxy = clientCache.getInitializedHSProxy();
    if (UserGroupInformation.isSecurityEnabled() && (hsProxy != null)) {
      /*
       * note that get delegation token was called. Again this is hack for oozie
       * to make sure we add history server delegation tokens to the credentials
       */
      RMDelegationTokenSelector tokenSelector = new RMDelegationTokenSelector();
      Text service = resMgrDelegate.getRMDelegationTokenService();
      if (tokenSelector.selectToken(service, ts.getAllTokens()) != null) {
        Text hsService = SecurityUtil.buildTokenService(hsProxy
            .getConnectAddress());
        if (ts.getToken(hsService) == null) {
          ts.addToken(hsService, getDelegationTokenFromHS(hsProxy));
        }
      }
    }
  }
  
  @VisibleForTesting
  Token<?> getDelegationTokenFromHS(MRClientProtocol hsProxy)
      throws IOException, InterruptedException {
    GetDelegationTokenRequest request = recordFactory
      .newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(conf));
    org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken = hsProxy.getDelegationToken(request)
        .getDelegationToken();
    return ConverterUtils.convertFromYarn(mrDelegationToken,
        hsProxy.getConnectAddress());
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException, InterruptedException {
    // The token is only used for serialization. So the type information
    // mismatch should be fine.
    return resMgrDelegate.getDelegationToken(renewer);
  }

  @Override
  public String getFilesystemName() throws IOException, InterruptedException {
    return resMgrDelegate.getFilesystemName();
  }

  @Override
  public JobID getNewJobID() throws IOException, InterruptedException {
    return resMgrDelegate.getNewJobID();
  }

  @Override
  public QueueInfo getQueue(String queueName) throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueue(queueName);
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueueAclsForCurrentUser();
  }

  @Override
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getQueues();
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getRootQueues();
  }

  @Override
  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    return resMgrDelegate.getChildQueues(parent);
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    return resMgrDelegate.getStagingAreaDir();
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    return resMgrDelegate.getSystemDir();
  }

  @Override
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return resMgrDelegate.getTaskTrackerExpiryInterval();
  }

  @Override
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
  throws IOException, InterruptedException {
	  
	  local_BFT_flag =conf.getInt("mapred.job.bft", 1);
	    local_NUM_REPLICAS =conf.getInt("mapred.job.numreplicas",4);
	    replicasHashes = new Long[conf.getInt("mapreduce.job.reduces", 1)];
	    replicasHashes_set = new int[conf.getInt("mapreduce.job.reduces", 1)/local_NUM_REPLICAS]; 
	    
	    System.out.println("INSIDE YARNRUNNER.java local_BFT_flag = "+local_BFT_flag);
	    System.out.println("INSIDE YARNRUNNER.java local_NUM_REPLICAS = "+local_NUM_REPLICAS);
	    System.out.println("INSIDE YARNRUNNER.java conf.getInt(\"mapreduce.job.reduces\", 1) = "+conf.getInt("mapreduce.job.reduces", 1));
	    

	  
	    if(local_BFT_flag==3)//case 3 start the verification thread .... TODO NEED TO ADD CASE 2
	    {
		    ThreadedEchoServer4=new Thread(new ThreadedEchoServer4());
		    ThreadedEchoServer4.setName("ThreadedEchoServer4 Thread");
		    ThreadedEchoServer4.start();
	    }

	  
	System.out.println("______3______submitJob inside YARNRunner.java___________here the MR AM starts_____and submits to the RM______ ");
    
    addHistoryToken(ts);
    
    // Construct necessary information to start the MR AM
    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(conf, jobSubmitDir, ts);

    // Submit to ResourceManager
    try {
      ApplicationId applicationId = resMgrDelegate.submitApplication(appContext);

      ApplicationReport appMaster = resMgrDelegate.getApplicationReport(applicationId);
      String diagnostics = (appMaster == null ? "application report is null" : appMaster.getDiagnostics());
      if (appMaster == null
          || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
          || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
        throw new IOException("Failed to run job : " + diagnostics);
      }
      return clientCache.getClient(jobId).getJobStatus(jobId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  private LocalResource createApplicationResource(FileContext fs, Path p, LocalResourceType type)
      throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
        .getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  public ApplicationSubmissionContext createApplicationSubmissionContext(
      Configuration jobConf,
      String jobSubmitDir, Credentials ts) throws IOException {
    ApplicationId applicationId = resMgrDelegate.getApplicationId();

    // Setup resource requirements
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(
        conf.getInt(
            MRJobConfig.MR_AM_VMEM_MB, MRJobConfig.DEFAULT_MR_AM_VMEM_MB
            )
        );
    capability.setVirtualCores(
        conf.getInt(
            MRJobConfig.MR_AM_CPU_VCORES, MRJobConfig.DEFAULT_MR_AM_CPU_VCORES
            )
        );
    LOG.debug("AppMaster capability = " + capability);

    // Setup LocalResources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JOB_CONF_FILE);

    URL yarnUrlForJobSubmitDir = ConverterUtils
        .getYarnUrlFromPath(defaultFileContext.getDefaultFileSystem()
            .resolvePath(
                defaultFileContext.makeQualified(new Path(jobSubmitDir))));
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    localResources.put(MRJobConfig.JOB_CONF_FILE,
        createApplicationResource(defaultFileContext,
            jobConfPath, LocalResourceType.FILE));
    if (jobConf.get(MRJobConfig.JAR) != null) {
      Path jobJarPath = new Path(jobConf.get(MRJobConfig.JAR));
      LocalResource rc = createApplicationResource(defaultFileContext,
          jobJarPath, 
          LocalResourceType.PATTERN);
      String pattern = conf.getPattern(JobContext.JAR_UNPACK_PATTERN, 
          JobConf.UNPACK_JAR_PATTERN_DEFAULT).pattern();
      rc.setPattern(pattern);
      localResources.put(MRJobConfig.JOB_JAR, rc);
    } else {
      // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
      // mapreduce jar itself which is already on the classpath.
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }

    // TODO gross hack
    for (String s : new String[] {
        MRJobConfig.JOB_SPLIT,
        MRJobConfig.JOB_SPLIT_METAINFO }) {
      localResources.put(
          MRJobConfig.JOB_SUBMIT_DIR + "/" + s,
          createApplicationResource(defaultFileContext,
              new Path(jobSubmitDir, s), LocalResourceType.FILE));
    }

    // Setup security tokens
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens  = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(MRApps.crossPlatformifyMREnv(jobConf, Environment.JAVA_HOME)
        + "/bin/java");

    // TODO: why do we use 'conf' some places and 'jobConf' others?
    long logSize = jobConf.getLong(MRJobConfig.MR_AM_LOG_KB,
        MRJobConfig.DEFAULT_MR_AM_LOG_KB) << 10;
    String logLevel = jobConf.get(
        MRJobConfig.MR_AM_LOG_LEVEL, MRJobConfig.DEFAULT_MR_AM_LOG_LEVEL);
    int numBackups = jobConf.getInt(MRJobConfig.MR_AM_LOG_BACKUPS,
        MRJobConfig.DEFAULT_MR_AM_LOG_BACKUPS);
    MRApps.addLog4jSystemProperties(logLevel, logSize, numBackups, vargs);

    // Check for Java Lib Path usage in MAP and REDUCE configs
    warnForJavaLibPath(conf.get(MRJobConfig.MAP_JAVA_OPTS,""), "map", 
        MRJobConfig.MAP_JAVA_OPTS, MRJobConfig.MAP_ENV);
    warnForJavaLibPath(conf.get(MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,""), "map", 
        MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS, MRJobConfig.MAPRED_ADMIN_USER_ENV);
    warnForJavaLibPath(conf.get(MRJobConfig.REDUCE_JAVA_OPTS,""), "reduce", 
        MRJobConfig.REDUCE_JAVA_OPTS, MRJobConfig.REDUCE_ENV);
    warnForJavaLibPath(conf.get(MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,""), "reduce", 
        MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS, MRJobConfig.MAPRED_ADMIN_USER_ENV);   

    // Add AM admin command opts before user command opts
    // so that it can be overridden by user
    String mrAppMasterAdminOptions = conf.get(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_ADMIN_COMMAND_OPTS);
    warnForJavaLibPath(mrAppMasterAdminOptions, "app master", 
        MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, MRJobConfig.MR_AM_ADMIN_USER_ENV);
    vargs.add(mrAppMasterAdminOptions);
    
    // Add AM user command opts
    String mrAppMasterUserOptions = conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS);
    warnForJavaLibPath(mrAppMasterUserOptions, "app master", 
        MRJobConfig.MR_AM_COMMAND_OPTS, MRJobConfig.MR_AM_ENV);
    vargs.add(mrAppMasterUserOptions);
    
    vargs.add(MRJobConfig.APPLICATION_MASTER_CLASS);
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        Path.SEPARATOR + ApplicationConstants.STDERR);


    Vector<String> vargsFinal = new Vector<String>(8);
    // Final command
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.debug("Command to launch container for ApplicationMaster is : "
        + mergedCommand);

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment, conf);

    // Setup the environment variables for Admin first
    MRApps.setEnvFromInputString(environment, 
        conf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV), conf);
    // Setup the environment variables (LD_LIBRARY_PATH, etc)
    MRApps.setEnvFromInputString(environment, 
        conf.get(MRJobConfig.MR_AM_ENV), conf);

    // Parse distributed cache
    MRApps.setupDistributedCache(jobConf, localResources);

    Map<ApplicationAccessType, String> acls
        = new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, jobConf.get(
        MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));
    acls.put(ApplicationAccessType.MODIFY_APP, jobConf.get(
        MRJobConfig.JOB_ACL_MODIFY_JOB,
        MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(localResources, environment,
          vargsFinal, null, securityTokens, acls);

    Collection<String> tagsFromConf =
        jobConf.getTrimmedStringCollection(MRJobConfig.JOB_TAGS);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId);                // ApplicationId
    appContext.setQueue(                                       // Queue name
        jobConf.get(JobContext.QUEUE_NAME,
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    appContext.setApplicationName(                             // Job name
        jobConf.get(JobContext.JOB_NAME,
        YarnConfiguration.DEFAULT_APPLICATION_NAME));
    appContext.setCancelTokensWhenComplete(
        conf.getBoolean(MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN, true));
    appContext.setAMContainerSpec(amContainer);         // AM Container
    appContext.setMaxAppAttempts(
        conf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS,
            MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));
    appContext.setResource(capability);
    appContext.setApplicationType(MRJobConfig.MR_APPLICATION_TYPE);
    if (tagsFromConf != null && !tagsFromConf.isEmpty()) {
      appContext.setApplicationTags(new HashSet<String>(tagsFromConf));
    }
    return appContext;
  }

  @Override
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    resMgrDelegate.setJobPriority(arg0, arg1);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return resMgrDelegate.getProtocolVersion(arg0, arg1);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Use Token.renew instead");
  }


  @Override
  public Counters getJobCounters(JobID arg0) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0).getJobCounters(arg0);
  }

  @Override
  public String getJobHistoryDir() throws IOException, InterruptedException {
    return JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
  }

  @Override
  public JobStatus getJobStatus(JobID jobID) throws IOException,
      InterruptedException {
    JobStatus status = clientCache.getClient(jobID).getJobStatus(jobID);
    return status;
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    return clientCache.getClient(arg0).getTaskCompletionEvents(arg0, arg1, arg2);
  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0.getJobID()).getTaskDiagnostics(arg0);
  }

  @Override
  public TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
  throws IOException, InterruptedException {
    return clientCache.getClient(jobID)
        .getTaskReports(jobID, taskType);
  }

  @Override
  public void killJob(JobID arg0) throws IOException, InterruptedException {
    /* check if the status is not running, if not send kill to RM */
    JobStatus status = clientCache.getClient(arg0).getJobStatus(arg0);
    if (status.getState() != JobStatus.State.RUNNING) {
      try {
        resMgrDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
      } catch (YarnException e) {
        throw new IOException(e);
      }
      return;
    }

    try {
      /* send a kill to the AM */
      clientCache.getClient(arg0).killJob(arg0);
      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L) && (status.getState()
          != JobStatus.State.KILLED)) {
          try {
            Thread.sleep(1000L);
          } catch(InterruptedException ie) {
            /** interrupted, just break */
            break;
          }
          currentTimeMillis = System.currentTimeMillis();
          status = clientCache.getClient(arg0).getJobStatus(arg0);
      }
    } catch(IOException io) {
      LOG.debug("Error when checking for application status", io);
    }
    if (status.getState() != JobStatus.State.KILLED) {
      try {
        resMgrDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
      } catch (YarnException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean killTask(TaskAttemptID arg0, boolean arg1) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0.getJobID()).killTask(arg0, arg1);
  }

  @Override
  public AccessControlList getQueueAdmins(String arg0) throws IOException {
    return new AccessControlList("*");
  }

  @Override
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
      InterruptedException {
    return JobTrackerStatus.RUNNING;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion,
        clientMethodsHash);
  }

  @Override
  public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException {
    return clientCache.getClient(jobID).getLogFilePath(jobID, taskAttemptID);
  }

  private static void warnForJavaLibPath(String opts, String component, 
      String javaConf, String envConf) {
    if (opts != null && opts.contains("-Djava.library.path")) {
      LOG.warn("Usage of -Djava.library.path in " + javaConf + " can cause " +
               "programs to no longer function if hadoop native libraries " +
               "are used. These values should be set as part of the " +
               "LD_LIBRARY_PATH in the " + component + " JVM env using " +
               envConf + " config settings.");
    }
  }
}
