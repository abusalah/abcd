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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//import multiclient.EchoThread;









import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;


import java.lang.Math;


/**
 * This class keeps track of tasks that have already been launched. It
 * determines if a task is alive and running or marks a task as dead if it does
 * not hear from it for a long time.
 * 
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskHeartbeatHandler extends AbstractService {
  
  private static class ReportTime {
    private long lastProgress;
    
    public ReportTime(long time) {
      setLastProgress(time);
    }
    
    public synchronized void setLastProgress(long time) {
      lastProgress = time;
    }

    public synchronized long getLastProgress() {
      return lastProgress;
    }
  }
  
  private static final Log LOG = LogFactory.getLog(TaskHeartbeatHandler.class);
  
  private static Long[] replicasHashes; //= new Long[MRJobConfig.NUM_REDUCES];
  private static int[] replicasHashes_set;
  
  static Socket clientSocket = null;
  static ServerSocket serverSocket = null;
  //static clientThread t[] = new clientThread[10];
  static ArrayList<clientThread> client_Threads_List = new ArrayList<clientThread>();  

  
  //thread which runs periodically to see the last time since a heartbeat is
  //received from a task.
  private Thread lostTaskCheckerThread;
  private Thread verifierThread;
  private static Thread ThreadedEchoServer4;
  private volatile boolean stopped;
  private int taskTimeOut = 1 * 60 * 1000;// 5 mins //////////////---bft //original was: taskTimeOut = 5 * 60 * 1000;
  private int taskTimeOutCheckInterval = 30 * 1000; // 30 seconds.

  private final EventHandler eventHandler;
  private final Clock clock;
  
  private ConcurrentMap<TaskAttemptId, ReportTime> runningAttempts;

  public TaskHeartbeatHandler(EventHandler eventHandler, Clock clock, int numThreads) {
    super("TaskHeartbeatHandler");
    this.eventHandler = eventHandler;
    this.clock = clock;
    runningAttempts = new ConcurrentHashMap<TaskAttemptId, ReportTime>(16, 0.75f, numThreads);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    taskTimeOut = conf.getInt(MRJobConfig.TASK_TIMEOUT, 1 * 60 * 1000);///---bft //original was: 5 * 60 * 1000;
    taskTimeOutCheckInterval = conf.getInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 30 * 1000);
    replicasHashes = new Long[conf.getInt(MRJobConfig.NUM_REDUCES, 1)];
    replicasHashes_set = new int[conf.getInt(MRJobConfig.NUM_REDUCES, 1)/4];
  }

  @Override
  protected void serviceStart() throws Exception {
	  System.out.println("___________inside serviceStart() in TaskHeartbeatHandler.java_______________Thread.currentThread().getStackTrace() = ");
	  for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {System.out.println("ste = "+ste);}
    lostTaskCheckerThread = new Thread(new PingChecker());
    lostTaskCheckerThread.setName("TaskHeartbeatHandler PingChecker");
    lostTaskCheckerThread.start();
    
    //verifierThread=new Thread(new Verifier());
    //verifierThread.setName("Verifier Thread");
    //verifierThread.start();
    
    ThreadedEchoServer4=new Thread(new ThreadedEchoServer4());
    ThreadedEchoServer4.setName("ThreadedEchoServer4 Thread");
    ThreadedEchoServer4.start();
    
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (lostTaskCheckerThread != null) {
      lostTaskCheckerThread.interrupt();
    }
    super.serviceStop();
  }

  public void progressing(TaskAttemptId attemptID) {
  //only put for the registered attempts
    //TODO throw an exception if the task isn't registered.
    ReportTime time = runningAttempts.get(attemptID);
    if(time != null) {
      time.setLastProgress(clock.getTime());
    }
  }

  
  public void register(TaskAttemptId attemptID) {	  
    runningAttempts.put(attemptID, new ReportTime(clock.getTime()));
  }

  public void unregister(TaskAttemptId attemptID) {
    runningAttempts.remove(attemptID);
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
    	
    	
      while (!stopped && !Thread.currentThread().isInterrupted()) {
    	  Iterator<Map.Entry<TaskAttemptId, ReportTime>> iterator = runningAttempts.entrySet().iterator();
        
    	  

        // avoid calculating current time everytime in loop
        long currentTime = clock.getTime();

        while (iterator.hasNext()) {
          Map.Entry<TaskAttemptId, ReportTime> entry = iterator.next();
          
          //System.out.println("-+ entry.getKey() = "+entry.getKey()+
          //	  " entry.getValue().getLastProgress() = "+entry.getValue().getLastProgress()+"");
          boolean taskTimedOut = (taskTimeOut > 0) && 
              (currentTime > (entry.getValue().getLastProgress() + taskTimeOut));
          
          
          //System.out.println("&&"+Integer.parseInt(entry.getKey().toString().split("_")[4]));
          
          /*
          if(Integer.parseInt(entry.getKey().toString().split("_")[4])==5)//attempt_1405535839415_0001_r_000005_2
          {
        	  iterator.remove();
              //eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(entry
              //    .getKey(), "AttemptID:" + entry.getKey().toString()
              //    + " ----____---- Timed out after " + taskTimeOut / 1000 + " secs"));
              eventHandler.handle(new TaskAttemptEvent(entry.getKey(),TaskAttemptEventType.TA_FAILMSG));        	  
          }
          */
          
           
          if(taskTimedOut) {
            // task is lost, remove from the list and raise lost event
            iterator.remove();
            eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(entry
                .getKey(), "AttemptID:" + entry.getKey().toString()
                + " ----____---- Timed out after " + taskTimeOut / 1000 + " secs"));
            eventHandler.handle(new TaskAttemptEvent(entry.getKey(),TaskAttemptEventType.TA_TIMED_OUT));
          }
        }
        //---IMP this was uncommented in the original code
        try {
          Thread.sleep(taskTimeOutCheckInterval);
        } catch (InterruptedException e) {
          LOG.info("TaskHeartbeatHandler thread interrupted");
          break;
        }
        
      }
    }
  }
  
  
  
  
  
  
  
    
  
  
  
  
  
  
  
  
  
  
//  public class EchoThread4 extends Thread {
//	    protected Socket socket;
//
//	    public EchoThread4(Socket clientSocket) {
//	        this.socket = clientSocket;
//	    }
//
//	    public void run() {
//	        InputStream inp = null;
//	        BufferedReader brinp = null;
//	        DataOutputStream out = null;
//	        
//	        int receivedReducerNumber=0;
//            String receivedTaskAttemptID ="";
//            long receivedHash= 0;
//            Integer unreplicatedReducerNumber=null;
//            boolean firstandsecond,thirdandforth,allofthem;
//            
//	        
//	        try {
//	            inp = socket.getInputStream();
//	            brinp = new BufferedReader(new InputStreamReader(inp));
//	            out = new DataOutputStream(socket.getOutputStream());
//	            
//	        } catch (IOException e) {
//	            return;
//	        }
//	        String line=null;
//	        while (true) {
//	            try {
//	                line = brinp.readLine();
//	                if ((line == null) || line.equalsIgnoreCase("QUIT")) {//
//	                    socket.close();
//	                    return;
//	                } else {
//	                	
//	                	
//	                	
//	                	
//	                	
//	                	
//	                	receivedReducerNumber = Integer.parseInt(line.split(" ")[0]);
//		                receivedTaskAttemptID = line.split(" ")[1];
//		                receivedHash = Long.parseLong(line.split(" ")[2]);
//		                unreplicatedReducerNumber = (int) Math.floor(receivedReducerNumber/4); 
//		                replicasHashes[receivedReducerNumber]=receivedHash;
//		                replicasHashes_set[unreplicatedReducerNumber]+=1;
//		                
//		                System.out.println("---------------------------------------------------------------------------");
//		                System.out.println("receivedReducerNumber = "+receivedReducerNumber+
//		                		"receivedTaskAttemptID = " + receivedTaskAttemptID +
//		                		"receivedHash = " + receivedHash +
//		                		"unreplicatedReducerNumber = "+unreplicatedReducerNumber
//		                		);
//		                
//		                
//		                  for(int i =0;i<replicasHashes.length;i++)
//		                  {
//		               	   System.out.println("replicasHashes i = "+i+" is "+replicasHashes[i]);
//		                  }
//		                  for(int i =0;i<replicasHashes_set.length;i++)
//		                  {
//		               	   System.out.println("replicasHashes_set i = "+i+" is "+replicasHashes_set[i]);
//		                  }
//		                System.out.println("---------------------------------------------------------------------------");
//		                  
//		                if(replicasHashes_set[unreplicatedReducerNumber]==4)
//		                {
//		             	   firstandsecond = (replicasHashes[(unreplicatedReducerNumber*4)+0] == replicasHashes[(unreplicatedReducerNumber*4)+1]);
//		             	   thirdandforth = (replicasHashes[(unreplicatedReducerNumber*4)+2] == replicasHashes[(unreplicatedReducerNumber*4)+3]);
//		             	   allofthem = (firstandsecond == thirdandforth);
//		             	   if (allofthem==true)
//		             	   {
//		             		   System.out.println("ALL CORRECT FOR REDUCER "+unreplicatedReducerNumber);
//		             		 //capitalizedSentence = clientSentence.toUpperCase() + '\n';
//		             		    System.out.println("line = "+line);
//			                    out.writeBytes(unreplicatedReducerNumber + "\n\r");
//			                    out.flush();
//			                	
//		             		  
//		             		   
//		             	   }
//		                }
//		                
//		                
//	                	
//	                	
//	                	
//	                	
//	                	
//	                	
//	                	
//	                	
//	                	//System.out.println("line = "+line);
//	                    //out.writeBytes(line+"123" + "\n\r");
//	                    //out.flush();
//	                }
//	            } catch (IOException e) {
//	                e.printStackTrace();
//	                return;
//	            }
//	        }
//	    }
//		
//	}
  
  
  public class ThreadedEchoServer4 implements Runnable {

	  
	  
	  public void run(){
			try {
				serverSocket = new ServerSocket(2222);
			} catch (IOException e) {
				System.out.println(e);
			}

			while (true) {
				try {
					clientSocket = serverSocket.accept();
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
				} catch (IOException e) {
					System.out.println(e);
				}
			}
		  
		  
	  }
	  
	  
	  
//	    public ThreadedEchoServer4(){}
//
//	    public void run() {
//	        ServerSocket serverSocket = null;
//	        Socket socket = null;
//
//	        try {
//	            serverSocket = new ServerSocket(2222);
//	        } catch (IOException e) {
//	            e.printStackTrace();
//
//	        }
//	        while (true) {
//	            try {
//	                socket = serverSocket.accept();
//	            } catch (IOException e) {
//	                System.out.println("I/O error: " + e);
//	            }
//	            // new threat for a client
//	            new EchoThread4(socket).start();//.run();
//	        }
//	    }
	}


  class clientThread extends Thread {

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
			//String name;
			try {
				is = new DataInputStream(clientSocket.getInputStream());
				os = new PrintStream(clientSocket.getOutputStream());
				
				
				while (true) {
					lineReceived = is.readLine();
					System.out.println(lineReceived);//NOTE the difference between os and System.out 
					
					
					receivedReducerNumber = Integer.parseInt(lineReceived.split(" ")[0]);
	                receivedTaskAttemptID = lineReceived.split(" ")[1];
	                receivedHash = Long.parseLong(lineReceived.split(" ")[2]);
	                unreplicatedReducerNumber = (int) Math.floor(receivedReducerNumber/4); 
	                replicasHashes[receivedReducerNumber]=receivedHash;
	                replicasHashes_set[unreplicatedReducerNumber]+=1;
	                
	                System.out.println("---------------------------------------------------------------------------");
	                System.out.println("receivedReducerNumber = "+receivedReducerNumber+
	                		"receivedTaskAttemptID = " + receivedTaskAttemptID +
	                		"receivedHash = " + receivedHash +
	                		"unreplicatedReducerNumber = "+unreplicatedReducerNumber
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
	                  
	                if(replicasHashes_set[unreplicatedReducerNumber]==4)//TODO make >=4 in case it is restarted from HeartBeats
	                {
	             	   firstandsecond = (replicasHashes[(unreplicatedReducerNumber*4)+0] == replicasHashes[(unreplicatedReducerNumber*4)+1]);
	             	   thirdandforth = (replicasHashes[(unreplicatedReducerNumber*4)+2] == replicasHashes[(unreplicatedReducerNumber*4)+3]);
	             	   allofthem = (firstandsecond == thirdandforth);
	             	   if (allofthem==true)
	             	   {
	             		   System.out.println("ALL CORRECT FOR REDUCER "+unreplicatedReducerNumber);
	             		for(clientThread x:client_Threads_List)
	   					{
	             			System.out.println("client_Threads_List.size() = "+client_Threads_List.size());
	   						x.os.println(unreplicatedReducerNumber);//unreplicatedReducerNumber);//x.os.println("XXXX");	   						
	   					}
	             		ii=0;
	             		for(clientThread x:client_Threads_List)
	   					{	
	   						receivedOK = is.readLine();
	   						if(receivedOK=="ok")
	   						{client_Threads_List.remove(ii);}
	   						ii++;
	   					}
	             		
	             		
	             		 //capitalizedSentence = clientSentence.toUpperCase() + '\n';
	             		    //System.out.println("lineReceived = "+lineReceived);
		                    //out.writeBytes(unreplicatedReducerNumber + "\n\r");
		                    //out.flush();
		                	
	             		  
	             		   
	             	   }
	                }
//	                for(clientThread x:client_Threads_List)
//   					{
//   						x.os.println();//unreplicatedReducerNumber);//x.os.println("XXXX");
//   					}
//             		
	                
	
					
					//for(clientThread x:client_Threads_List)
					{
						//os.println("XXXX");//x.os.println("XXXX");
					}
					if (lineReceived.startsWith("ww"))
						break;
					
				}
				
				
				//for (int i = 0; i <= 9; i++)
				//	if (t[i] != null && t[i] != this)
				//		t[i].os.println("*** The user is leaving the chat room !!! ***");//t[i].os.println("*** The user " + name + " is leaving the chat room !!! ***");

				//os.println("*** Bye " + name + " ***");

				//for (int i = 0; i <= 9; i++)
					//if (t[i] == this)
						//t[i] = null;
				is.close();
				os.close();
				clientSocket.close();
			} catch (IOException e) {
			}
			;
		}
	}



  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
//  
//  
//  public class ThreadedEchoServer implements Runnable {
//
//	    static final int PORT = 6789;
//
//	    public void run() {
//	        ServerSocket serverSocket = null;
//	        Socket socket = null;
//
//	        try {
//	            serverSocket = new ServerSocket(PORT);
//	        } catch (IOException e) {
//	            e.printStackTrace();
//
//	        }
//	        while (true) {
//	            try {
//	                socket = serverSocket.accept();
//	            } catch (IOException e) {
//	                System.out.println("I/O error: " + e);
//	            }
//	            // new threat for a client
//	            new EchoThread(socket).start();//.run();
//	        }
//	    }
//	}
//
//  
//  public class EchoThread extends Thread {
//	    protected Socket socket;
//
//	    public EchoThread(Socket clientSocket) {
//	        this.socket = clientSocket;
//	    }
//
//	    public void run() {
//	        InputStream inp = null;
//	        BufferedReader brinp = null;
//	        DataOutputStream out = null;
//	        BufferedReader inFromClient = null;
//	        DataOutputStream outToClient = null;
//	        
//	        String clientSentence = null;
//            String capitalizedSentence;
//            ServerSocket welcomeSocket = null;
//            
//            int receivedReducerNumber=0;
//            String receivedTaskAttemptID ="";
//            long receivedHash= 0;
//            Integer unreplicatedReducerNumber=null;
//            boolean firstandsecond,thirdandforth,allofthem;
//            
//	        
//	        try {
//	            inp = socket.getInputStream();
//	            brinp = new BufferedReader(new InputStreamReader(inp));	            
//	            out = new DataOutputStream(socket.getOutputStream());
//	            inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//	            outToClient = new DataOutputStream(socket.getOutputStream());	            
//	        } catch (IOException e) {
//	            return;
//	        }
//	        String line;
//	        while (true) {
//	            try {
//	                clientSentence = inFromClient.readLine();
//	                
//	                receivedReducerNumber = Integer.parseInt(clientSentence.split(" ")[0]);
//	                receivedTaskAttemptID = clientSentence.split(" ")[1];
//	                receivedHash = Long.parseLong(clientSentence.split(" ")[2]);
//	                unreplicatedReducerNumber = (int) Math.floor(receivedReducerNumber/4); 
//	                replicasHashes[receivedReducerNumber]=receivedHash;
//	                replicasHashes_set[unreplicatedReducerNumber]+=1;
//	                
//	                if(replicasHashes_set[unreplicatedReducerNumber]==4)
//	                {
//	             	   firstandsecond = (replicasHashes[(unreplicatedReducerNumber*4)+0] == replicasHashes[(unreplicatedReducerNumber*4)+1]);
//	             	   thirdandforth = (replicasHashes[(unreplicatedReducerNumber*4)+2] == replicasHashes[(unreplicatedReducerNumber*4)+3]);
//	             	   allofthem = (firstandsecond == thirdandforth);
//	             	   if (allofthem==true)
//	             	   {
//	             		   System.out.println("ALL CORRECT FOR REDUCER "+unreplicatedReducerNumber);
//	             		 //capitalizedSentence = clientSentence.toUpperCase() + '\n';
//	             		   
//	                        try {
//	 						outToClient.writeBytes(unreplicatedReducerNumber.toString()+"\n\r");	 					     
//	 	                    out.flush();
//	 					} catch (IOException e) {
//	 						// TODO Auto-generated catch block
//	 						e.printStackTrace();
//	 					}
//	             	   }
//	                }
//	                /*
//	                line = brinp.readLine();
//	                if ((line == null) || line.equalsIgnoreCase("QUIT")) {//
//	                    socket.close();
//	                    return;
//	                } else {
//	                	System.out.println("line = "+line);
//	                    out.writeBytes(line + "\n\r");
//	                    out.flush();
//	                }
//	                */
//	            } catch (IOException e) {
//	                e.printStackTrace();
//	                return;
//	            }
//	        }
//	    }
//		
//	}
//  
//  
//  //old tcp server
//  private class Verifier implements Runnable {
//
//	    @Override
//	    public void run() {
//	    	String clientSentence = null;
//            String capitalizedSentence;
//            ServerSocket welcomeSocket = null;
//            
//            int receivedReducerNumber=0;
//            String receivedTaskAttemptID ="";
//            long receivedHash= 0;
//            Integer unreplicatedReducerNumber=null;
//            boolean firstandsecond,thirdandforth,allofthem;
//             
//			try {
//				welcomeSocket = new ServerSocket(6789);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//            while(true)
//            {
//               Socket connectionSocket = null;
//				try {
//					connectionSocket = welcomeSocket.accept();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//               BufferedReader inFromClient = null;
//               DataOutputStream outToClient = null;
//				try {
//					inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//               try {
//					outToClient = new DataOutputStream(connectionSocket.getOutputStream());
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//               try {
//					clientSentence = inFromClient.readLine();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//               System.out.println("Inside TaskHeartbeatHandler Received: " + clientSentence);
//               receivedReducerNumber = Integer.parseInt(clientSentence.split(" ")[0]);
//               receivedTaskAttemptID = clientSentence.split(" ")[1];
//               receivedHash = Long.parseLong(clientSentence.split(" ")[2]);
//               unreplicatedReducerNumber = (int) Math.floor(receivedReducerNumber/4); 
//               replicasHashes[receivedReducerNumber]=receivedHash;
//               replicasHashes_set[unreplicatedReducerNumber]+=1;
//               
//               if(replicasHashes_set[unreplicatedReducerNumber]==4)
//               {
//            	   firstandsecond = (replicasHashes[(unreplicatedReducerNumber*4)+0] == replicasHashes[(unreplicatedReducerNumber*4)+1]);
//            	   thirdandforth = (replicasHashes[(unreplicatedReducerNumber*4)+2] == replicasHashes[(unreplicatedReducerNumber*4)+3]);
//            	   allofthem = (firstandsecond == thirdandforth);
//            	   if (allofthem==true)
//            	   {
//            		   System.out.println("ALL CORRECT FOR REDUCER "+unreplicatedReducerNumber);
//            		 //capitalizedSentence = clientSentence.toUpperCase() + '\n';
//                       try {
//						outToClient.writeBytes(unreplicatedReducerNumber.toString());
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//            	   }
//               }
//               
//               
//               for(int i =0;i<replicasHashes.length;i++)
//               {
//            	   System.out.println("replicasHashes i = "+i+" is "+replicasHashes[i]);
//               }
//               for(int i =0;i<replicasHashes_set.length;i++)
//               {
//            	   System.out.println("replicasHashes_set i = "+i+" is "+replicasHashes_set[i]);
//               }
//               
//            }
//	    }
//	  }
//
//  
//  
  
  
  
  
  
  
  
  
  
  
  
  
  

}
