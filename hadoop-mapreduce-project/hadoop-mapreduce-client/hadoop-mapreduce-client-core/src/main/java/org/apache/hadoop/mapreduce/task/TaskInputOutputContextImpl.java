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

package org.apache.hadoop.mapreduce.task;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A context object that allows input and output from the task. It is only
 * supplied to the {@link Mapper} or {@link Reducer}.
 * @param <KEYIN> the input key type for the task
 * @param <VALUEIN> the input value type for the task
 * @param <KEYOUT> the output key type for the task
 * @param <VALUEOUT> the output value type for the task
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
       extends TaskAttemptContextImpl 
       implements TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private RecordWriter<KEYOUT,VALUEOUT> output;
  private OutputCommitter committer;
  
//was static
	public Socket clientSocket = null;
	public PrintStream os = null;
	public DataInputStream is = null;
	public BufferedReader inputLine = null;
	private static boolean closed = false;
	
	private final Object lock = new Object();
	
	public String local_taskID = null;
  
  

  public TaskInputOutputContextImpl(Configuration conf, TaskAttemptID taskid,
                                    RecordWriter<KEYOUT,VALUEOUT> output,
                                    OutputCommitter committer,
                                    StatusReporter reporter) {
	  
    super(conf, taskid, reporter);
    this.output = output;
    this.committer = committer;
    this.local_taskID=taskid.toString();
  }

  /**
   * Advance to the next key, value pair, returning null if at end.
   * @return the key object that was read into, or null if no more
   */
  public abstract 
  boolean nextKeyValue() throws IOException, InterruptedException;
 
  /**
   * Get the current key.
   * @return the current key object or null if there isn't one
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract 
  KEYIN getCurrentKey() throws IOException, InterruptedException;

  /**
   * Get the current value.
   * @return the value object that was read into
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract VALUEIN getCurrentValue() throws IOException, 
                                                   InterruptedException;

  /**
   * Generate an output key/value pair.
   */
  public void write(KEYOUT key, VALUEOUT value
                    ) throws IOException, InterruptedException {
	  System.out.println("this.local_taskID = "+this.local_taskID);
	  System.out.println("++++++ inside write in TaskInputOutputContextImpl key.toString() = "
                    +key.toString()+"value.toString() = "+value.toString());
	 
    output.write(key, value);
    
//    if(conf.getInt(MRJobConfig.BFT_FLAG, 1)==3)//TODO NEED TO ADD CASE 2
//    {
//		
//  	  int local_NUM_REPLICAS = conf.getInt(MRJobConfig.NUM_REPLICAS,4); 
//  	  String reducerORmapper = this.local_taskID.split("_")[3];
//  	  int reducerNumber = Integer.parseInt(this.local_taskID.split("_")[4]);
//  	  int unreplicatedReducerNumber = (int) Math.floor(reducerNumber/local_NUM_REPLICAS);
//  	  
//  	 
//  	  
//      
//      try {
//      	String KV=""; int i=0; long totalHash=0; String stringToSend=""; String stringReceived="";
//      	//System.out.println("+++ entered try");
//      	while (context.nextKey()) {
//          reduce(context.getCurrentKey(), context.getValues(), context);
//          
//          if(reducerORmapper.equals("r"))
//          {
//  	        //KV+=context.getCurrentKey().toString()+context.getCurrentValue().toString();// first hashing method
//  	        KV=context.getCurrentKey().toString()+context.getCurrentValue().toString();
//  	        totalHash+=KV.hashCode();
//  	        //System.out.println("key = "+context.getCurrentKey()+" value = "+context.getCurrentValue()+
//  	        //		" KV.hashCode() = "+KV.hashCode()+" totalHash = "+totalHash);
//  	        //KV="p";
//          }
//          
//          // If a back up store is used, reset it
//          Iterator<VALUEIN> iter = context.getValues().iterator();
//          if(iter instanceof ReduceContext.ValueIterator) {((ReduceContext.ValueIterator<VALUEIN>)iter).resetBackupStore();}  
//          
//            
//          i++;
//        }
//        
//        
//        
//        if(reducerORmapper.equals("r"))
//        {
//  	      
//      	  System.out.println("ENTERED if(reducerORmapper.equals(\"r\"))");
//      	  
//      	  totalHash=0;//just for now for testing    	  
//      	  stringToSend=reducerNumber+" "+this.local_taskID+" "+totalHash;
//      	  
//      	  
//      	  try {
//    			clientSocket = new Socket("mc07.cs.purdue.edu", 2222);//("mc07.cs.purdue.edu", 2222);
//    			inputLine = new BufferedReader(new InputStreamReader(System.in));
//    			os = new PrintStream(clientSocket.getOutputStream());
//    			is = new DataInputStream(clientSocket.getInputStream());
//    		} catch (UnknownHostException e) {
//    			System.err.println("Don't know about host mc07.cs.purdue.edu");
//    		} catch (IOException e) {
//    			System.err.println("Couldn't get I/O for the connection to the host mc07.cs.purdue.edu");
//    			System.out.println("e.getMessage() = "+e.getMessage());
//    			System.out.println("e.toString() = "+e.toString());
//    			System.out.println("e.getCause() = "+e.getCause());  			
//    		}
//
//    		
//    		if (clientSocket != null && os != null && is != null) {
//    			try {
//
//    				os.println(stringToSend);
//    				String responseLine;
//    				System.out.println("Before while");
//    				while(true){
//    					System.out.println("Entered while");
//  					responseLine = is.readLine();
//  					System.out.println("responseLine = "+responseLine);
//  					if(responseLine!=null && !responseLine.isEmpty())
//  					{
//  						//add if stmt for checking the server address, but first open a socket here for each Reducer for accepting server address
//  						//clientSocket = serverSocket.accept();(put it above)
//  						if (Integer.parseInt(responseLine)==unreplicatedReducerNumber)
//  						{	
//  							System.out.println("Entered XXX------");
//  							break;
//  						}
//  					}
//    				}  				
//    				/* WORKING PERFECTLY .... need to uncomment class MultiThreadChatClient
//    				 // Create a thread to read from the server
//    				new Thread(new MultiThreadChatClient(unreplicatedReducerNumber)).start();//try sending is,closed if this didn't work
//    				os.println(stringToSend);
//    				
//    				 while (true) {
//    					synchronized(lock){//CHECK IF THIS CAUSES AN OVERHEAD
//    					if(closed)
//    						{
//    							System.out.println("ENTERED if(closed)");
//    							break;
//    						}
//    					}
//    				}*/
//    				//os.println("ok");
//    				System.out.println("AFTER THE TWO WHILES");
//    				os.close();
//    				is.close();
//    				clientSocket.close();
//    			} 
//    			catch (IOException e) {
//    				System.err.println("IOException:  " + e);
//    			}
//    		}
//  		  
//  		  	      
//  		  KV="";stringToSend="";totalHash=0;
//        }
//        
//        
//      
//      } 
//    
//    }
    
  }

  public OutputCommitter getOutputCommitter() {
    return committer;
  }
}
