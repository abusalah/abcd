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
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;
import java.net.InetAddress;
 



public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);

	  //System.out.println("this.local_taskID = "+this.local_taskID);
	  //System.out.println("++++++ inside write in IntSumReducer class in WordCount.java key.toString() = "
      //              +key.toString()+" result = "+result);
    }
  }
  
  
  
//  public static class MyRunnable implements Runnable {
//
//	    private Job local_job;
//
//	    public MyRunnable(Job sent_job) {
//	    	System.out.println("Inside 1 sent_job.getJar() = "+sent_job.getJar());
//	        this.local_job = sent_job;
//	        System.out.println("Inside 2 sent_job.getJar() = "+sent_job.getJar());
//	    }
//
//	    public void run() {
//	    	try {
//	    		System.out.println("Inside 3 this.local_job.getJar() = "+this.local_job.getJar());
//	    		this.local_job=new Job(this.local_job.getConfiguration(), "word count");
//				this.local_job.waitForCompletion(true);
//				
//			} catch (ClassNotFoundException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}//.waitForCompletion(true);
//	        // code in the other thread, can reference "var" variable
//	    }
//	}
//


  public static void main(String[] args) throws Exception {
	  int r3=0;//default//number of AM replicas
	  int BFT_FLAG_LOCAL = 0;
	  
	  try {//---- mapred-site.xml parser // new for bft
      	File fXmlFile = new File("etc/hadoop/mapred-site.xml");
      	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      	Document doc = dBuilder.parse(fXmlFile);
       	doc.getDocumentElement().normalize();
       	NodeList nList = doc.getElementsByTagName("property");
       	for (int temp = 0; temp < nList.getLength(); temp++) {
       		Node nNode = nList.item(temp);
       		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
       			Element eElement = (Element) nNode;
      			if(eElement.getElementsByTagName("name").item(0).getTextContent().equals("mapred.job.bft"))
      			{
      				System.out.println(".........name : " + eElement.getElementsByTagName("name").item(0).getTextContent());
      				System.out.println(".........value : " + eElement.getElementsByTagName("value").item(0).getTextContent());
      				BFT_FLAG_LOCAL=Integer.parseInt(eElement.getElementsByTagName("value").item(0).getTextContent().toString());
      			}
      		}
      	}
          } catch (Exception e) {
      	e.printStackTrace();
          }
	  
	     switch (BFT_FLAG_LOCAL) 
		{
	        case 1://No BFT
	        {
	        	System.out.println("------ENTERED case 1---------");
	        	r3=1;
	        	break;
	        }
	        case 2://BFT: replicate the AM(it should replicate the mappers and reducers by itself)   //deal with it as No BFT
	        {
	        	System.out.println("------ENTERED case 2---------");
	        	r3=4;
	        	break;	        
	        }
	        case 3://BFT: replicate mappers and reducers (both r times ?), single AM
	        {
	        	System.out.println("------ENTERED case 3---------");
	        	r3=1;
	        	break;
	        }
	        case 4://BFT: replicate the AM (r3 times in WordCount.java) and replicate mappers and reducers (both r times)
	        {
	        	System.out.println("------ENTERED case 4---------");
	        	r3=4;
	        	break;	        
	        }
	        default://deal with it as No BFT
	        {
	        	System.out.println("------ENTERED default---------");
	        	r3=1;
	        	break;
	        }
		}
	    
	    
	    Configuration[] conf = new Configuration[r3];
	    for( int i=0; i<r3; i++ )
	    {
	    	conf[i] = new Configuration();
	    		
	    }
	    	
	    
	    String[] otherArgs = new GenericOptionsParser(conf[0], args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    
	    
	    
	    long startTime = System.currentTimeMillis()/1000;
	    long elapsedTime = 0L;
	    		
	  for (int i=0;i<r3;i++)
	  {
		  System.out.println("------INSIDE the for loop , r3 = --------- "+r3+" -------------- ");
		  
		  Job job = new Job(conf[i], "word count");
		  
		  System.out.println("job.getJobID() = "+job.getJobID()+" job.getJobName() = "+job.getJobName());
		  
		  
		  
		  
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+Integer.toString(i)));
	    //System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	    
	    switch (BFT_FLAG_LOCAL) 
		{
	        case 1://No BFT
	        {
	        	System.out.println("------in WordCount.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
	        	job.waitForCompletion(true);
	        	break;
	        }
	        case 2://BFT: replicate the AM(it should replicate the mappers and reducers by itself)   //deal with it as No BFT
	        {
	        	System.out.println("------in WordCount.java----job.submit();-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
	        	job.submit();
	        	break;	        
	        }
	        case 3://BFT: replicate mappers and reducers (both r times ?), single AM
	        {
	        	System.out.println("------in WordCount.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
	        	job.waitForCompletion(true);
	        	break;
	        }
	        case 4://BFT: replicate the AM (r3 times in WordCount.java) and replicate mappers and reducers (both r times)
	        {
	        	//Not used
	        	break;	        
	        }
	        default://deal with it as No BFT
	        {
	        	System.out.println("------in WordCount.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL is in default case");
	        	job.waitForCompletion(true);
	        	break;
	        }
		}
	    //job.submit();
	    
	   
//	    System.out.println("Before job.getJar() = "+job.getJar());
//	    
//	    MyRunnable myRunnable = new MyRunnable(job);
//        Thread t = new Thread(myRunnable);
//        t.start();
	    
	    //job.waitForCompletion(true);//was true
	    

	    
//	    try {
//	        Thread.sleep(10000);
//	    } catch(InterruptedException ex) {
//	        Thread.currentThread().interrupt();
//	    }
	    
	    
	  }
	  elapsedTime = System.currentTimeMillis()/1000 - startTime;//elapsedTime = (new Date()).getTime() - startTime;	  
	  System.out.println("\n\n----------- elapsedTime in seconds = "+elapsedTime+"\n\n");
	  
	  //for (int iiii=0;iiii<conf[0].replicasHashes_333_set.length;iiii++)
      {
    	  //System.out.print(" conf[0].replicasHashes_333_set = "+iiii+" is "+conf[0].replicasHashes_333_set[iiii]);
      }
	  
  }
}






/*//---bft : this is the the Original Code
public static void main(String[] args) throws Exception {
	  
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
}
*/









