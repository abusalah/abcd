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

package org.apache.hadoop.mapreduce;

import java.io.*;
import java.net.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.task.annotation.Checkpointable;
//import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
//import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.event.EventHandler;
//import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;

//import org.apache.hadoop.mapreduce.v2dfdfd//.app;//
//import org.apache.hadoop.mapreduce.v2.app.*;


import org.apache.hadoop.ipc.Server;












import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/** 
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.  
 * 
 * <p><code>Reducer</code> implementations 
 * can access the {@link Configuration} for the job via the 
 * {@link JobContext#getConfiguration()} method.</p>

 * <p><code>Reducer</code> has 3 primary phases:</p>
 * <ol>
 *   <li>
 *   
 *   <h4 id="Shuffle">Shuffle</h4>
 *   
 *   <p>The <code>Reducer</code> copies the sorted output from each 
 *   {@link Mapper} using HTTP across the network.</p>
 *   </li>
 *   
 *   <li>
 *   <h4 id="Sort">Sort</h4>
 *   
 *   <p>The framework merge sorts <code>Reducer</code> inputs by 
 *   <code>key</code>s 
 *   (since different <code>Mapper</code>s may have output the same key).</p>
 *   
 *   <p>The shuffle and sort phases occur simultaneously i.e. while outputs are
 *   being fetched they are merged.</p>
 *      
 *   <h5 id="SecondarySort">SecondarySort</h5>
 *   
 *   <p>To achieve a secondary sort on the values returned by the value 
 *   iterator, the application should extend the key with the secondary
 *   key and define a grouping comparator. The keys will be sorted using the
 *   entire key, but will be grouped using the grouping comparator to decide
 *   which keys and values are sent in the same call to reduce.The grouping 
 *   comparator is specified via 
 *   {@link Job#setGroupingComparatorClass(Class)}. The sort order is
 *   controlled by 
 *   {@link Job#setSortComparatorClass(Class)}.</p>
 *   
 *   
 *   For example, say that you want to find duplicate web pages and tag them 
 *   all with the url of the "best" known example. You would set up the job 
 *   like:
 *   <ul>
 *     <li>Map Input Key: url</li>
 *     <li>Map Input Value: document</li>
 *     <li>Map Output Key: document checksum, url pagerank</li>
 *     <li>Map Output Value: url</li>
 *     <li>Partitioner: by checksum</li>
 *     <li>OutputKeyComparator: by checksum and then decreasing pagerank</li>
 *     <li>OutputValueGroupingComparator: by checksum</li>
 *   </ul>
 *   </li>
 *   
 *   <li>   
 *   <h4 id="Reduce">Reduce</h4>
 *   
 *   <p>In this phase the 
 *   {@link #reduce(Object, Iterable, Context)}
 *   method is called for each <code>&lt;key, (collection of values)&gt;</code> in
 *   the sorted inputs.</p>
 *   <p>The output of the reduce task is typically written to a 
 *   {@link RecordWriter} via 
 *   {@link Context#write(Object, Object)}.</p>
 *   </li>
 * </ol>
 * 
 * <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.</p>
 * 
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class IntSumReducer&lt;Key&gt; extends Reducer&lt;Key,IntWritable,
 *                                                 Key,IntWritable&gt; {
 *   private IntWritable result = new IntWritable();
 * 
 *   public void reduce(Key key, Iterable&lt;IntWritable&gt; values,
 *                      Context context) throws IOException, InterruptedException {
 *     int sum = 0;
 *     for (IntWritable val : values) {
 *       sum += val.get();
 *     }
 *     result.set(sum);
 *     context.write(key, result);
 *   }
 * }
 * </pre></blockquote></p>
 * 
 * @see Mapper
 * @see Partitioner
 */
@Checkpointable
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	
	//was static
	public Socket clientSocket = null;
	public PrintStream os = null;
	public DataInputStream is = null;
	public BufferedReader inputLine = null;
	private static boolean closed = false;
	
	private final Object lock = new Object();
	
	public static int finalValue=0;
	
	
   /**
   * The <code>Context</code> passed on to the {@link Reducer} implementations.
   */
  public abstract class Context implements ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

	 
  }

  /**
   * Called once at the start of the task.
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
	  
    // NOTHING
  }

  /**
   * This method is called once for each key. Most applications will define
   * their reduce class by overriding this method. The default implementation
   * is an identity function.
   */
  @SuppressWarnings("unchecked")
  protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context
                        ) throws IOException, InterruptedException {
	  //System.out.println("ENTERED reduce in Reducerclass ");
    for(VALUEIN value: values) {
    	//System.out.println("+++___+++ key.toString() = " +key.toString()+" value.toString() = "+value.toString());
      context.write((KEYOUT) key, (VALUEOUT) value);
    }
  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Advanced application writers can use the 
   * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
   * control how the reduce task works.
   */
  public void run(Context context) throws IOException, InterruptedException {
	  
	  
if(context.getConfiguration().getInt(MRJobConfig.BFT_FLAG, 1)==1)	 
{
	    setup(context);
	    try {
	      while (context.nextKey()) {
	        reduce(context.getCurrentKey(), context.getValues(), context);
	        // If a back up store is used, reset it
	        Iterator<VALUEIN> iter = context.getValues().iterator();
	        if(iter instanceof ReduceContext.ValueIterator) {
	          ((ReduceContext.ValueIterator<VALUEIN>)iter).resetBackupStore();
	        }
	      }
	    } finally {
	      cleanup(context);
	    }
}
	  
	  
	  
if(context.getConfiguration().getInt(MRJobConfig.BFT_FLAG, 1)==3)//TODO NEED TO ADD CASE 2
{
	
		
	  int local_NUM_REPLICAS = context.getConfiguration().getInt(MRJobConfig.NUM_REPLICAS,4); 
	  String reducerORmapper = context.getTaskAttemptID().toString().split("_")[3];
	  int reducerNumber = Integer.parseInt(context.getTaskAttemptID().toString().split("_")[4]);
	  int unreplicatedReducerNumber = (int) Math.floor(reducerNumber/local_NUM_REPLICAS);
	  
	 
	  //System.out.println("ENTERED run in Reducer.java");
	  
    setup(context);
    
    try {
    	String KV=""; int i=0; long totalHash=0; String stringToSend=""; String stringReceived="";
    	//System.out.println("+++ entered try");
    	while (context.nextKey()) {
    		
    		System.out.println("context.nextKeyValue = "+context.nextKeyValue());
    		
    		KEYIN xxx = context.getCurrentKey();
    		System.out.println("context.nextKey() = "+context.nextKey());
    		
    		if(context.nextKey()==false)
            {finalValue=1;System.out.println("Entered if(context.nextKey()==false) finalValue = "+finalValue);}
        reduce(xxx, context.getValues(), context);
        
        if(reducerORmapper.equals("r"))
        {
	        //KV+=context.getCurrentKey().toString()+context.getCurrentValue().toString();// first hashing method
	        KV=context.getCurrentKey().toString()+context.getCurrentValue().toString();
	        totalHash+=KV.hashCode();
	        //System.out.println("key = "+context.getCurrentKey()+" value = "+context.getCurrentValue()+
	        //		" KV.hashCode() = "+KV.hashCode()+" totalHash = "+totalHash);
	        //KV="p";
        }
        
        // If a back up store is used, reset it
        Iterator<VALUEIN> iter = context.getValues().iterator();
        if(iter instanceof ReduceContext.ValueIterator) {((ReduceContext.ValueIterator<VALUEIN>)iter).resetBackupStore();}  
        
        
          
        i++;
      }
      
      
      
      if(reducerORmapper.equals("r"))
      {
	      
    	  System.out.println("ENTERED if(reducerORmapper.equals(\"r\"))");
    	  
    	  totalHash=0;//just for now for testing    	  
    	  stringToSend=reducerNumber+" "+context.getTaskAttemptID().toString()+" "+totalHash;
    	  
    	  
    	  try {
  			clientSocket = new Socket("mc07.cs.purdue.edu", 2222);//("mc07.cs.purdue.edu", 2222);
  			inputLine = new BufferedReader(new InputStreamReader(System.in));
  			os = new PrintStream(clientSocket.getOutputStream());
  			is = new DataInputStream(clientSocket.getInputStream());
  		} catch (UnknownHostException e) {
  			System.err.println("Don't know about host mc07.cs.purdue.edu");
  		} catch (IOException e) {
  			System.err.println("Couldn't get I/O for the connection to the host mc07.cs.purdue.edu");
  			System.out.println("e.getMessage() = "+e.getMessage());
  			System.out.println("e.toString() = "+e.toString());
  			System.out.println("e.getCause() = "+e.getCause());  			
  		}

  		
  		if (clientSocket != null && os != null && is != null) {
  			try {

  				os.println(stringToSend);
  				String responseLine;
  				System.out.println("Before while");
  				while(true){
  					System.out.println("Entered while");
					responseLine = is.readLine();
					System.out.println("responseLine = "+responseLine);
					if(responseLine!=null && !responseLine.isEmpty())
					{
						//add if stmt for checking the server address, but first open a socket here for each Reducer for accepting server address
						//clientSocket = serverSocket.accept();(put it above)
						if (Integer.parseInt(responseLine)==unreplicatedReducerNumber)
						{	
							System.out.println("Entered XXX------");
							break;
						}
					}
  				}  				
  				/* WORKING PERFECTLY .... need to uncomment class MultiThreadChatClient
  				 // Create a thread to read from the server
  				new Thread(new MultiThreadChatClient(unreplicatedReducerNumber)).start();//try sending is,closed if this didn't work
  				os.println(stringToSend);
  				
  				 while (true) {
  					synchronized(lock){//CHECK IF THIS CAUSES AN OVERHEAD
  					if(closed)
  						{
  							System.out.println("ENTERED if(closed)");
  							break;
  						}
  					}
  				}*/
  				//os.println("ok");
  				System.out.println("AFTER THE TWO WHILES");
  				os.close();
  				is.close();
  				clientSocket.close();
  			} 
  			catch (IOException e) {
  				System.err.println("IOException:  " + e);
  			}
  		}
		  
		  	      
		  KV="";stringToSend="";totalHash=0;
      }
      
      
      
    } finally {    	
      cleanup(context);
    }
  }
}
  
}  
  
//  //works with the above WORKING PERFECTLY ... needs to be inside class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> brackets 
//  public class MultiThreadChatClient extends Thread {
//	  int unreplicatedReducerNumber_ForClass;
//	  
//	  public MultiThreadChatClient(int unreplicatedReducerNumber_Local){
//		  this.unreplicatedReducerNumber_ForClass=unreplicatedReducerNumber_Local;
//	 }
//	  
//	  public void run() {
//			String responseLine;
//			System.out.println("Before while");
//			try {
//				while (true){//((responseLine = is.readLine()) != null) {
//					System.out.println("Entered while");
//					responseLine = is.readLine();
//					System.out.println("responseLine = "+responseLine);
//					if (Integer.parseInt(responseLine)==this.unreplicatedReducerNumber_ForClass)//indexOf("*** Bye") != -1)
//					{	
//						System.out.println("Entered XXX------");
//						break;
//					}
//				}
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			synchronized (lock) {
//				closed = true;
//			}
//			
//			System.out.println("in MultiThreadChatClient closed = "+ closed);
//		}
//	  
//  }
  
  



//
//Socket clientSocket;
//String sentence;
//String modifiedSentence=null;
////BufferedReader inFromUser = new BufferedReader( new InputStreamReader(System.in));
//
//System.out.println("starting TCPClient");
//while(true)
//{
//	   clientSocket = new Socket("localhost", 6789);
//
//
//	      DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
//		  BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//		  //while(true)
//		  
//			  sentence = stringToSend;
//			  outToServer.writeBytes(sentence + '\n' );//
//			  if(sentence.equals("o")){break;}
//			  
//			  modifiedSentence = inFromServer.readLine();
//			  System.out.println("FROM SERVER-------: " + modifiedSentence);
//			  
//			  if(Integer.parseInt(modifiedSentence)==unreplicatedReducerNumber)
//				  {
//				  	System.out.println("Received OK ------------------------");
//				  	break;
//				  }
//		  
//	  }
//clientSocket.close();
