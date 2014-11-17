package VThreadPackage;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

public class VThread {
	


public static PrintWriter writer;

    public static int bft2VerifierThreadFlag=0;
  
    private static String[] replicasHashes;//private static Long[] replicasHashes; //= new Long[MRJobConfig.NUM_REDUCES];
    private static int[] replicasHashes_set;
    public static int resetArraysFlagCount=0;//a count to know when to reset replicasHashes and replicasHashes_set arrays
    public static int resetArraysFlag=0;
    public static int num_verified_apps=0;
    public static long startTime=0;
    public static long elapsedTime=0L;    
    public static String allreducers ="";
    private static Long[] temp_replicasHashes_forbft2 = new Long[2];
    private static List<Long> temp_replicasHashes_forbft2_LIST = new ArrayList<Long>();
    private static Map<Integer, Long> temp_replicasHashes_forbft2_MAP = new HashMap<Integer, Long>();

    //private static Long[] replicasHashes_forbft2;
    //private static String[] applicationsNames;
    //private static Map<String, List<Long>> AMsMap = new HashMap<String, List<Long>>();
    private static Map<String, Map<Integer, String>> AMsMap = new HashMap<String, Map<Integer, String>>();//new HashMap<String, Map<Integer, Long>>();
    private static Map<String, Integer> hash_sum_per_App_replica = new HashMap<String, Integer>();//new HashMap<String, Long>();
    private static Map<String, Integer> ApplicationNumberBase_Touch = new HashMap<String, Integer>();

    private static int local_BFT_flag=0;
    private static int local_NUM_REPLICAS = 0;
    private static int local_NUM_REDUCES = 0;
  
    public static Thread VerifierThread;
    public static boolean stopFlag=false;
    public static Socket clientSocket = null;
    public static ServerSocket serverSocket = null;
  
    //bft_______new code________________________________________________________________________________________________________
  
  
    //static clientThread t[] = new clientThread[10];
    public static ArrayList<clientThread> client_Threads_List = new ArrayList<clientThread>();  
  
    public static class VerifierThreadClass implements Runnable {
	  
	public void run(){
		
	      
	      
	    try {//just to write the output to a file
		writer = new PrintWriter("outputfileVerifierThreadClass", "UTF-8");
	    } catch (FileNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	    } catch (UnsupportedEncodingException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	    }
	      
	    System.out.println("inside run()");
	    //temp_replicasHashes_forbft2_LIST = new ArrayList<Long>(local_NUM_REDUCES);
	    try {
		serverSocket = new ServerSocket(2226);
	    } catch (IOException e) {
		System.out.println("\n\n\n\nserverSocket Exception\n\n\n");
		System.out.println(e);
	    }
	      
	    System.out.println("after serverSocket = new ServerSocket(2226);");
	    
	    while (true) {//!Thread.currentThread().isInterrupted()
		try {
		    System.out.println("inside try inside while (true) inside VerifierThreadClass class");
		    clientSocket = serverSocket.accept();
		    System.out.println("inside try inside while (true) inside VerifierThreadClass class AFTER clientSocket = serverSocket.accept();");
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
	    //try {
	    //System.out.println("ENTERED before serverSocket.close() ");
	    //serverSocket.close();
	    //System.out.println("ENTERED after serverSocket.close() ");
	    //} catch (IOException e) {
	    //// TODO Auto-generated catch block
	    //e.printStackTrace();
	    //}
	    //System.out.println("END OF interruption of VerifierThreadClass thread ");
	      
	      
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
	String receivedOutputPath="";
	long receivedApplicationNumber_1 =0;
	int receivedApplicationNumber_2 =0;
	String ApplicationNumberwithReplicaNumber_withOutputName=null;
	String ApplicationNumberBase_withOutputName=null;
	String receivedHash= null;
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
		System.out.println(" "+clientSocket.getInetAddress()+" "+clientSocket.getRemoteSocketAddress()+" "+
				   clientSocket.getLocalAddress()+" "+clientSocket.getPort()+" "+clientSocket.getLocalPort());
		is = new DataInputStream(clientSocket.getInputStream());
		os = new PrintStream(clientSocket.getOutputStream());
		System.out.println("Inside try inside run() inside clientThread class");
		
		while (true) {//(!stopped && !Thread.currentThread().isInterrupted())
		    lineReceived = is.readLine();//NOTE the difference between os and System.out
		    if(lineReceived!=null && !lineReceived.isEmpty())
			{
			    System.out.println("lineReceived from reducer = "+lineReceived); 
			    writer.println("lineReceived from reducer = "+lineReceived); 
			    
			    local_BFT_flag = Integer.parseInt(lineReceived.split(" ")[0].split("-")[0]);//conf.getInt("mapred.job.bft", 1);
			    local_NUM_REPLICAS = Integer.parseInt(lineReceived.split(" ")[0].split("-")[1]);//conf.getInt("mapred.job.numreplicas",4);
			    local_NUM_REDUCES = Integer.parseInt(lineReceived.split(" ")[0].split("-")[2]);//conf.getInt("mapreduce.job.reduces",1); 
			    if(resetArraysFlag==0)//new application for bft=3, need new arrays
			    {
			    	System.out.println("ENTERED if(resetArraysFlag==0)");
				    replicasHashes = new String[local_NUM_REDUCES];//new Long[local_NUM_REDUCES];
				    replicasHashes_set = new int[local_NUM_REDUCES/local_NUM_REPLICAS]; 
				    resetArraysFlag=1;
			    }
			    
			    System.out.println("INSIDE local_BFT_flag = "+local_BFT_flag);
			    System.out.println("INSIDE local_NUM_REPLICAS = "+local_NUM_REPLICAS);
			    System.out.println("INSIDE local_NUM_REDUCES = "+local_NUM_REDUCES);
			    

			    
			    
			    //receivedReducerNumber = Integer.parseInt(lineReceived.split(" ")[0]);
			    receivedOutputPath = lineReceived.split(" ")[1];
			    receivedTaskAttemptID = lineReceived.split(" ")[2];		
			    receivedHash = lineReceived.split(" ")[3];//Long.parseLong(lineReceived.split(" ")[3]);
			    receivedReducerNumber = Integer.parseInt(receivedTaskAttemptID.toString().split("_")[4]);
			    receivedApplicationNumber_1=Long.parseLong(receivedTaskAttemptID.toString().split("_")[1]);
			    receivedApplicationNumber_2=Integer.parseInt(receivedTaskAttemptID.toString().split("_")[2]);
			    ApplicationNumberwithReplicaNumber_withOutputName = receivedOutputPath.substring(0, receivedOutputPath.length()-1)+
			    		"_"+Long.toString(receivedApplicationNumber_1)+
			    		"_"+Integer.toString(receivedApplicationNumber_2);
			    ApplicationNumberBase_withOutputName=receivedOutputPath.substring(0, receivedOutputPath.length()-1)+"_"+Long.toString(receivedApplicationNumber_1);
			    
			    System.out.println("\n"+receivedOutputPath+" "+receivedTaskAttemptID+" "+receivedHash+" "+receivedReducerNumber+" "+receivedApplicationNumber_1
			    		+" "+receivedApplicationNumber_2+" "+ApplicationNumberwithReplicaNumber_withOutputName+" "+ApplicationNumberBase_withOutputName+"\n");
			    
			    if(local_BFT_flag==3)
				{
				    System.out.println("ENTERED local_BFT_flag==3");
				    unreplicatedReducerNumber = (int) Math.floor(receivedReducerNumber/local_NUM_REPLICAS); 
				    System.out.println("unreplicatedReducerNumber = "+unreplicatedReducerNumber);
				    System.out.println("replicasHashes.length = "+replicasHashes.length);
				    System.out.println("replicasHashes_set.length = "+replicasHashes_set.length);
				    replicasHashes[receivedReducerNumber]=receivedHash;
				    replicasHashes_set[unreplicatedReducerNumber]+=1;
				    //--- This is just for testing received 3 hashes
				    if(replicasHashes_set[unreplicatedReducerNumber]>=3)
				    {
				    	allreducers+="\nRECEIVED AT LEAST 3 CORRECT hashes for reducer "+unreplicatedReducerNumber+" \n";
				    	System.out.println("\n\n allreducers \n\n");
				    	elapsedTime = System.currentTimeMillis()/1000 - startTime;//elapsedTime = (new Date()).getTime() - startTime;	  
		    			System.out.println("\n\n----------- elapsedTime in seconds = "+elapsedTime+"\n\n");
				    }
			    	//--- End                 
				   
				    System.out.println("---------------------------------PRINTING------------------------------------------");
				    System.out.println("receivedReducerNumber = "+receivedReducerNumber+
						   "receivedTaskAttemptID = " + receivedTaskAttemptID +
						   "receivedHash = " + receivedHash +
						   "unreplicatedReducerNumber = "+unreplicatedReducerNumber+
						   "ApplicationName = "+ApplicationNumberwithReplicaNumber_withOutputName
						   );
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
					{//Note that the above line should be == local_NUM_REPLICAS if you want 4/4 replicas to be correct. >=local_NUM_REPLICAS-1 to make it 3/4 replicas are enough
				    	resetArraysFlagCount++;
				    	System.out.println("resetArraysFlagCount = "+resetArraysFlagCount);
				    	System.out.println("local_NUM_REDUCES/local_NUM_REPLICAS = "+local_NUM_REDUCES/local_NUM_REPLICAS);
				    	if(resetArraysFlagCount==local_NUM_REDUCES/local_NUM_REPLICAS)//which is basically the original number of reducers
				    	{
				    		System.out.println("ENTERED if(resetArraysFlagCount==local_NUM_REDUCES/local_NUM_REPLICAS)");
				    		resetArraysFlag=0;
				    		resetArraysFlagCount=0;
				    	}
				    	allofthem=false;
					    for(int i=0;i<local_NUM_REPLICAS-1;i++)//NOTE ... that it is from 0 to <local_NUM_REPLICAS-1 ... which means 0 to =local_NUM_REPLICAS-2  
						{
						    //writer.println("replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i] = "+replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i]);
						    //writer.println("replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1] = "+replicasHashes[(unreplicatedReducerNumber*local_NUM_REPLICAS)+i+1]);
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
					    //writer.flush();
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
						      writer.println("RECEIVED OK FROM ii = "+ii);
						      writer.println("BEFORE client_Threads_List.size() = "+client_Threads_List.size());
						      client_Threads_List.remove(ii);
						      writer.println("AFTER client_Threads_List.size() = "+client_Threads_List.size());
						      }
						      ii++;
						      }
						    */
						    System.out.println("=========AFTER x.os.println(unreplicatedReducerNumber)=============");
						    
						    
						    //capitalizedSentence = clientSentence.toUpperCase() + '\n';
						    //writer.println("lineReceived = "+lineReceived);
						    //out.writeBytes(unreplicatedReducerNumber + "\n\r");
						    //out.flush();
						    
						      
						       
						}
					    
					    if(resetArraysFlag==0)//new application for bft=3, need new arrays
					    {
					    	System.out.println("ENTERED if(resetArraysFlag==0)");
						    replicasHashes = new String[local_NUM_REDUCES];//new Long[local_NUM_REDUCES];
						    replicasHashes_set = new int[local_NUM_REDUCES/local_NUM_REPLICAS]; 
						    resetArraysFlag=1;
					    }
					    
					}
				                    
				}
			    if(local_BFT_flag==2)
				{
				    System.out.println("ENTERED local_BFT_flag==2");
				    
				    if(ApplicationNumberBase_Touch.containsKey(ApplicationNumberBase_withOutputName))
				    {
						ApplicationNumberBase_Touch.put(ApplicationNumberBase_withOutputName, ApplicationNumberBase_Touch.get(ApplicationNumberBase_withOutputName)+1);
				    }
				    else
				    {
					    ApplicationNumberBase_Touch.put(ApplicationNumberBase_withOutputName, 1);//first time to see the app, put 1 touch
				    }
				    
				    if(AMsMap.containsKey(ApplicationNumberwithReplicaNumber_withOutputName))//we have the application
					{
					    System.out.println("ENTERED if(AMsMap.containsKey(ApplicationName))");
					    //if(AMsMap.get(ApplicationName) != null)//this application has received reducers before
					    {
						//temp_replicasHashes_forbft2_MAP = AMsMap.get(ApplicationName).put(receivedReducerNumber, receivedHash);
						AMsMap.get(ApplicationNumberwithReplicaNumber_withOutputName).put(receivedReducerNumber, receivedHash);
						hash_sum_per_App_replica.put(ApplicationNumberwithReplicaNumber_withOutputName, hash_sum_per_App_replica.get(ApplicationNumberwithReplicaNumber_withOutputName)+receivedHash.hashCode());
						System.out.println("---22");
						System.out.println("AMsMap.get(ApplicationName).size() = "+AMsMap.get(ApplicationNumberwithReplicaNumber_withOutputName).size());
						//temp_replicasHashes_forbft2_MAP.put(receivedReducerNumber, receivedHash);
						//System.out.println("temp_replicasHashes_forbft2_MAP.size() = "+temp_replicasHashes_forbft2_MAP.size());
						//temp_replicasHashes_forbft2[receivedReducerNumber]=receivedHash;
						System.out.println("---33");
						//AMsMap.put(ApplicationName, temp_replicasHashes_forbft2_MAP);
					    //temp_replicasHashes_forbft2_MAP=AMsMap.get(ApplicationName);
					    //System.out.println("temp_replicasHashes_forbft2_MAP.size() = "+temp_replicasHashes_forbft2_MAP.size());
						System.out.println("---44");
						//temp_replicasHashes_forbft2_MAP.clear();
						System.out.println("---55");
					    }
					    
					}
				    else//first time to see the application, add it to the hashmap
					{
				    	Map<Integer, String> new_replicasHashes_forbft2_MAP = new HashMap<Integer, String>();//new HashMap<Integer, Long>();
				    	
					    System.out.println("ENTERED if(AMsMap.containsKey(ApplicationName))  ....   else");
					    new_replicasHashes_forbft2_MAP.put(receivedReducerNumber, receivedHash);
					    //System.out.println("temp_replicasHashes_forbft2_MAP.size() = "+temp_replicasHashes_forbft2_MAP.size());
					    //temp_replicasHashes_forbft2[receivedReducerNumber]=receivedHash;
					    System.out.println("---2");
					    AMsMap.put(ApplicationNumberwithReplicaNumber_withOutputName, new_replicasHashes_forbft2_MAP);
					    hash_sum_per_App_replica.put(ApplicationNumberwithReplicaNumber_withOutputName, receivedHash.hashCode());//.hashCode() just to change string to int to be able to sum it above
					    //AMsMap.get(ApplicationName).put(receivedReducerNumber, receivedHash);
					    //temp_replicasHashes_forbft2_MAP=AMsMap.get(ApplicationName);
					    System.out.println("AMsMap.get(ApplicationNumberwithReplicaNumber).size() = "
					    +AMsMap.get(ApplicationNumberwithReplicaNumber_withOutputName).size());
					    System.out.println("---3");
					    //temp_replicasHashes_forbft2_MAP.clear();
					    System.out.println("---4");
					   
					}
				    
				    System.out.println("------------------------------------PRINTING---------------------------------------");
				    System.out.println("receivedReducerNumber = "+receivedReducerNumber+
						   " receivedTaskAttemptID = " + receivedTaskAttemptID +
						   " receivedHash = " + receivedHash +
						   " ApplicationNumberwithReplicaNumber = "+ApplicationNumberwithReplicaNumber_withOutputName+
						   " AMsMap.size()"+AMsMap.size()
						   );
				    for (Map.Entry<String, Map<Integer, String>> AppEntry: AMsMap.entrySet())
					{
					    System.out.println("AppEntry.getKey() = "+AppEntry.getKey());
					    //temp_replicasHashes_forbft2_MAP=AppEntry.getValue();
					    System.out.println("AppEntry.getValue().size() = "+AppEntry.getValue().size());
					    for(Map.Entry<Integer, String> AppEntry2: AppEntry.getValue().entrySet())//(int i =0;i<AppEntry.getValue().size();i++)
						{
						    //writer.println("temp_replicasHashes_forbft2_MAP.get(i) i = "+i+" is "+temp_replicasHashes_forbft2_MAP.get(i));
						    System.out.println("AppEntry2.getKey() = "+AppEntry2.getKey()+" AppEntry2.getValue() = "+AppEntry2.getValue());
						}
					    //temp_replicasHashes_forbft2_MAP.clear();          					    
					}
				    for (Map.Entry<String, Integer> hash_sum_per_App_Entery: hash_sum_per_App_replica.entrySet())
				    {
				    	System.out.println("hash_sum_per_App_Entery.getKey() = "+hash_sum_per_App_Entery.getKey()+" hash_sum_per_App_Entery.getValue() = "+hash_sum_per_App_Entery.getValue());
				    }
				    
				    System.out.println("---------------------------------------------------------------------------");
				    
				    
				    System.out.println("ApplicationNumberBase_Touch.get(ApplicationNumberBase_withOutputName) = "
				    +ApplicationNumberBase_Touch.get(ApplicationNumberBase_withOutputName));
				    
				    //TODO : should be ==local_NUM_REDUCES*local_NUM_REPLICAS in case you have different replication factors
				    //if(Collections.frequency(new ArrayList<String>(hash_sum_per_App_replica.keySet()), ApplicationNumberBase)==4) 
				    if(ApplicationNumberBase_Touch.get(ApplicationNumberBase_withOutputName)==local_NUM_REDUCES*4)//TODO: make it generic to num_replicas
				    {
				    	System.out.println("ALL hashes received, start comparing and sending ... for Application = "+ApplicationNumberBase_withOutputName);
				    	int q=0;//hash_sum_per_App loop variable
				    	//Map.Entry<String,int> entry = new AbstractMap.SimpleEntry<String, int>("exmpleString", 42);
				    	Map.Entry<String,Integer> tempEntry = new AbstractMap.SimpleEntry<String, Integer>(" ",0);//("exmpleString", (long)42);
				    	allofthem=false;
				    	System.out.println("-----");
				    	System.out.println("hash_sum_per_App_replica.size() = "+hash_sum_per_App_replica.size());
				    	for (Map.Entry<String, Integer> hash_sum_per_App_Entery: hash_sum_per_App_replica.entrySet())
				    	{//in case we have many applications running in parallel; check for the ApplicationNumberBase first
				    		
				    		System.out.println("hash_sum_per_App_Entery.getKey().split(\"_\")[0] = "+hash_sum_per_App_Entery.getKey().split("_")[0]);
				    		System.out.println("ApplicationNumberBase_withOutputName.split(\"_\")[0] = "+ApplicationNumberBase_withOutputName.split("_")[0]);
				    		System.out.println("hash_sum_per_App_Entery.getKey().split(\"_\")[1] = "+hash_sum_per_App_Entery.getKey().split("_")[1]);
				    		System.out.println("ApplicationNumberBase_withOutputName.split(\"_\")[1] = "+ApplicationNumberBase_withOutputName.split("_")[1]);
				    		
				    		
				    		if(hash_sum_per_App_Entery.getKey().split("_")[0].equals(ApplicationNumberBase_withOutputName.split("_")[0])
				    				&& hash_sum_per_App_Entery.getKey().split("_")[1].equals(ApplicationNumberBase_withOutputName.split("_")[1]))
				    		{
				    			System.out.println("ENTERED if(hash_sum_per_App_Entery.getKey().split(.....) equals(ApplicationNumberBase_withOutputName))");
				    			if(q==0)
				    			{
				    				System.out.println("ENTERED if(q==0)");
				    				tempEntry=hash_sum_per_App_Entery;
				    				num_verified_apps++;//TODO: make it per application, cuz assume that you have mutiple independent applications running in the cluster
				    				System.out.println("\nnum_verified_apps++\n");
				    				q++;
				    			}
				    			else//tempEntry has the prev value
				    			{ 
				    				System.out.println("ENTERED if(q==0) ... else");
				    				if(tempEntry.getValue().equals(hash_sum_per_App_Entery.getValue()))
				    				{
				    					System.out.println("ENTERED if(tempEntry.getValue().equals(hash_sum_per_App_Entery.getValue()))");
				    					tempEntry=hash_sum_per_App_Entery;//for next iteration
				    					num_verified_apps++;//TODO: make it per application, cuz assume that you have mutiple independent applications running in the cluster
				    					System.out.println("\nnum_verified_apps++\n");
				    					if(num_verified_apps>=3)//TODO: make it generic to num_replicas - 1
							    		{
							    			System.out.println("\n\nRECEIVED AT LEAST 3 CORRECT APPLICATIONS\n\n");
							    			elapsedTime = System.currentTimeMillis()/1000 - startTime;//elapsedTime = (new Date()).getTime() - startTime;	  
							    			System.out.println("\n\n----------- elapsedTime in seconds = "+elapsedTime+"\n\n");
							    			num_verified_apps=0;//for next set of application replicas				    			
							    		}				   
				    					allofthem=true;
				    				}
				    				else
				    				{
				    					System.out.println("ENTERED if(tempEntry.getValue().equals(hash_sum_per_App_Entery.getValue())) ... else");
				    					allofthem=false;
				    					break;
				    				}
				    			}
				    		}
				    	}
				    	
				    	if (allofthem==true)
						{ 		
						    System.out.println("ALL CORRECT FOR APPLICATION "+ApplicationNumberBase_withOutputName);
						    for(clientThread x:client_Threads_List)
							{
							    System.out.println("client_Threads_List.size() = "+client_Threads_List.size());
							    x.os.println(ApplicationNumberBase_withOutputName);//unreplicatedReducerNumber);//x.os.println("XXXX");   
							}
						}
				    }
				    
				    
				    
				                    
				}
			}
		                    
		                    
		                      
		                    
		    //if(lineReceived!=null && !lineReceived.isEmpty())
		    {
			if (lineReceived.startsWith("ww"))//TODO NEED TO HAVE A BETTER WAY TO CLOSE THE THREAD
			    break;
		    }
		    
		    
		    //is.close();
		    //os.close();
		    //clientSocket.close();
		}
	    } catch (IOException e) {
	    }
	    ;
	}
    }



    //bft_______end new code____________________________________________________________________________________________________


	

	public static void main(String[] args) {
		
		System.out.println("------ENTERED VThread--------");
		startTime = System.currentTimeMillis()/1000;
		System.out.println("------Timer Started--------");
	    
	  
	  
		
		
	  //if(bft2VerifierThreadFlag==0)//just to launch one Verifier Thread
	  {
		  //System.out.println("ENTERED if(bft2VerifierThreadFlag==0)");
		  //writer.println("ENTERED if(bft2VerifierThreadFlag==0)");
		  //writer.flush();
	    //if(local_BFT_flag==3 || local_BFT_flag==2)//case 3 start the verification thread .... TODO NEED TO ADD CASE 2
	    {
		    VerifierThread=new Thread(new VerifierThreadClass());
		    VerifierThread.setName("Verifier Thread");
		    VerifierThread.start();
	    }
	    //bft2VerifierThreadFlag=1;
	  }
	  
		
		
	}

}
