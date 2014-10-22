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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This is the trivial map/reduce program that does absolutely nothing
 * other than use the framework to fragment and sort the input values.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar sort
 *            [-r <i>reduces</i>]
 *            [-inFormat <i>input format class</i>] 
 *            [-outFormat <i>output format class</i>] 
 *            [-outKey <i>output key class</i>] 
 *            [-outValue <i>output value class</i>] 
 *            [-totalOrder <i>pcnt</i> <i>num samples</i> <i>max splits</i>]
 *            <i>in-dir</i> <i>out-dir</i> 
 */
public class Sort<K,V> extends Configured implements Tool {
  public static final String REDUCES_PER_HOST = 
    "mapreduce.sort.reducesperhost";
  private Job job = null;

  static int printUsage() {
    System.out.println("sort [-r <reduces>] " +
                       "[-inFormat <input format class>] " +
                       "[-outFormat <output format class>] " + 
                       "[-outKey <output key class>] " +
                       "[-outValue <output value class>] " +
                       "[-totalOrder <pcnt> <num samples> <max splits>] " +
                       "<input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return 2;
  }

  /**
   * The main driver for sort program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public int run(String[] args) throws Exception {
	  
	  
	  
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
		    	conf[i]= getConf();
		    		
		    }
	    

    //Configuration conf[0] = getConf();
    JobClient client = new JobClient(conf[0]);
    ClusterStatus cluster = client.getClusterStatus();
    int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.9);
    String sort_reduces = conf[0].get(REDUCES_PER_HOST);
    if (sort_reduces != null) {
       num_reduces = cluster.getTaskTrackers() * 
                       Integer.parseInt(sort_reduces);
    }
    Class<? extends InputFormat> inputFormatClass = 
      SequenceFileInputFormat.class;
    Class<? extends OutputFormat> outputFormatClass = 
      SequenceFileOutputFormat.class;
    Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
    Class<? extends Writable> outputValueClass = BytesWritable.class;
    List<String> otherArgs = new ArrayList<String>();
    InputSampler.Sampler<K,V> sampler = null;
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          num_reduces = Integer.parseInt(args[++i]);
        } else if ("-inFormat".equals(args[i])) {
          inputFormatClass = 
            Class.forName(args[++i]).asSubclass(InputFormat.class);
        } else if ("-outFormat".equals(args[i])) {
          outputFormatClass = 
            Class.forName(args[++i]).asSubclass(OutputFormat.class);
        } else if ("-outKey".equals(args[i])) {
          outputKeyClass = 
            Class.forName(args[++i]).asSubclass(WritableComparable.class);
        } else if ("-outValue".equals(args[i])) {
          outputValueClass = 
            Class.forName(args[++i]).asSubclass(Writable.class);
        } else if ("-totalOrder".equals(args[i])) {
          double pcnt = Double.parseDouble(args[++i]);
          int numSamples = Integer.parseInt(args[++i]);
          int maxSplits = Integer.parseInt(args[++i]);
          if (0 >= maxSplits) maxSplits = Integer.MAX_VALUE;
          sampler =
            new InputSampler.RandomSampler<K,V>(pcnt, numSamples, maxSplits);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage(); // exits
      }
    }
    
    
    
    for (int i=0;i<r3;i++)
	  {
		  System.out.println("------INSIDE the for loop , r3 = --------- "+r3+" -------------- ");
		  
		  //Job job = new Job(conf[i], "word count");
		  
		  
		  
		  
		  
		  // Set user-supplied (possibly default) job configs
		    job = new Job(conf[i]);
		    job.setJobName("sorter");
		    job.setJarByClass(Sort.class);

		    System.out.println("job.getJobID() = "+job.getJobID()+" job.getJobName() = "+job.getJobName());
			
		    job.setMapperClass(Mapper.class);        
		    job.setReducerClass(Reducer.class);

		    job.setNumReduceTasks(num_reduces);

		    job.setInputFormatClass(inputFormatClass);
		    job.setOutputFormatClass(outputFormatClass);

		    job.setOutputKeyClass(outputKeyClass);
		    job.setOutputValueClass(outputValueClass);

		  
	    
		    if (otherArgs.size() != 2) {
		        System.out.println("ERROR: Wrong number of parameters: " +
		            otherArgs.size() + " instead of 2.");
		        return printUsage();
		      }
		      FileInputFormat.setInputPaths(job, otherArgs.get(0));
		      FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
		      
		      if (sampler != null) {
		        System.out.println("Sampling input to effect total-order sort...");
		        job.setPartitionerClass(TotalOrderPartitioner.class);
		        Path inputDir = FileInputFormat.getInputPaths(job)[0];
		        inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf[0]));
		        Path partitionFile = new Path(inputDir, "_sortPartitioning");
		        TotalOrderPartitioner.setPartitionFile(conf[0], partitionFile);
		        InputSampler.<K,V>writePartitionFile(job, sampler);
		        URI partitionUri = new URI(partitionFile.toString() +
		                                   "#" + "_sortPartitioning");
		        DistributedCache.addCacheFile(partitionUri, conf[0]);
		      }

		      System.out.println("Running on " +
		          cluster.getTaskTrackers() +
		          " nodes to sort from " + 
		          FileInputFormat.getInputPaths(job)[0] + " into " +
		          FileOutputFormat.getOutputPath(job) +
		          " with " + num_reduces + " reduces.");
		      Date startTime = new Date();
		      System.out.println("Job started: " + startTime);

		    
		    
	    
	    switch (BFT_FLAG_LOCAL) 
		{
	        case 1://No BFT
	        {
	        	System.out.println("------in Sort.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
	        	job.waitForCompletion(true);
	        	break;
	        }
	        case 2://BFT: replicate the AM(it should replicate the mappers and reducers by itself)   //deal with it as No BFT
	        {
	        	System.out.println("------in Sort.java----job.submit();-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
	        	job.submit();
	        	break;	        
	        }
	        case 3://BFT: replicate mappers and reducers (both r times ?), single AM
	        {
	        	System.out.println("------in Sort.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
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
	        	System.out.println("------in Sort.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL is in default case");
	        	job.waitForCompletion(true);
	        	break;
	        }
		}
	    
	    
	    Date end_time = new Date();
	    System.out.println("Job ended: " + end_time);
	    System.out.println("The job took " + 
	        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
	    
	  }
    
    
//    // Set user-supplied (possibly default) job configs
//    job = new Job(conf[0]);
//    job.setJobName("sorter");
//    job.setJarByClass(Sort.class);
//
//    job.setMapperClass(Mapper.class);        
//    job.setReducerClass(Reducer.class);
//
//    job.setNumReduceTasks(num_reduces);
//
//    job.setInputFormatClass(inputFormatClass);
//    job.setOutputFormatClass(outputFormatClass);
//
//    job.setOutputKeyClass(outputKeyClass);
//    job.setOutputValueClass(outputValueClass);

    // Make sure there are exactly 2 parameters left.
//    if (otherArgs.size() != 2) {
//      System.out.println("ERROR: Wrong number of parameters: " +
//          otherArgs.size() + " instead of 2.");
//      return printUsage();
//    }
//    FileInputFormat.setInputPaths(job, otherArgs.get(0));
//    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
//    
//    if (sampler != null) {
//      System.out.println("Sampling input to effect total-order sort...");
//      job.setPartitionerClass(TotalOrderPartitioner.class);
//      Path inputDir = FileInputFormat.getInputPaths(job)[0];
//      inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf[0]));
//      Path partitionFile = new Path(inputDir, "_sortPartitioning");
//      TotalOrderPartitioner.setPartitionFile(conf[0], partitionFile);
//      InputSampler.<K,V>writePartitionFile(job, sampler);
//      URI partitionUri = new URI(partitionFile.toString() +
//                                 "#" + "_sortPartitioning");
//      DistributedCache.addCacheFile(partitionUri, conf[0]);
//    }
//
//    System.out.println("Running on " +
//        cluster.getTaskTrackers() +
//        " nodes to sort from " + 
//        FileInputFormat.getInputPaths(job)[0] + " into " +
//        FileOutputFormat.getOutputPath(job) +
//        " with " + num_reduces + " reduces.");
//    Date startTime = new Date();
//    System.out.println("Job started: " + startTime);
//    int ret = job.waitForCompletion(true) ? 0 : 1;
//    Date end_time = new Date();
//    System.out.println("Job ended: " + end_time);
//    System.out.println("The job took " + 
//        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    return 0;//return ret;
  }



  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Sort(), args);
    System.exit(res);
  }

  /**
   * Get the last job that was run using this instance.
   * @return the results of the last job that was run
   */
  public Job getResult() {
    return job;
  }
}
