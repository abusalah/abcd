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

package org.apache.hadoop.examples.terasort;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.jcraft.jsch.Random;





/**
 * Generates the sampled split points, launches the job, and waits for it to
 * finish. 
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar terasort in-dir out-dir</b>
 */
public class TeraSort extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(TeraSort.class);
  static String SIMPLE_PARTITIONER = "mapreduce.terasort.simplepartitioner";
  static String OUTPUT_REPLICATION = "mapreduce.terasort.output.replication";
  
  public static int r3=0;
  public static int BFT_FLAG_LOCAL = 0;
  
  public static class Globals {
	    public static int r3_global=0;
	    public static int BFT_FLAG_LOCAL_global=0;
	}


  /**
   * A partitioner that splits text keys into roughly equal partitions
   * in a global sorted order.
   */
  static class TotalOrderPartitioner extends Partitioner<Text,Text>
      implements Configurable {
    private TrieNode trie;
    private Text[] splitPoints;
    private Configuration conf;

    /**
     * A generic trie node
     */
    static abstract class TrieNode {
      private int level;
      TrieNode(int level) {
        this.level = level;
      }
      abstract int findPartition(Text key);
      abstract void print(PrintStream strm) throws IOException;
      int getLevel() {
        return level;
      }
    }

    /**
     * An inner trie node that contains 256 children based on the next
     * character.
     */
    static class InnerTrieNode extends TrieNode {
      private TrieNode[] child = new TrieNode[256];
      
      InnerTrieNode(int level) {
        super(level);
        //System.out.println("in InnerTrieNode level = "+level);
      }
      int findPartition(Text key) {
    	  //System.out.println("\n\nENTERED findPartition 1 in TeraSort.java \n\n");
        int level = getLevel();
        //System.out.println("in findPartition in InnerTrieNode level = "+level);
        if (key.getLength() <= level) {
        	int ret1 = child[0].findPartition(key);
        	//System.out.println("ret1 = "+ret1);
          return ret1;
        }
        int ret2 = child[key.getBytes()[level] & 0xff].findPartition(key); 
        //System.out.println("ret2 = "+ret2);
        return ret2;
      }
      void setChild(int idx, TrieNode child) {
        this.child[idx] = child;
      }
      void print(PrintStream strm) throws IOException {
        for(int ch=0; ch < 256; ++ch) {
          for(int i = 0; i < 2*getLevel(); ++i) {
            strm.print(' ');
          }
          strm.print(ch);
          strm.println(" ->");
          if (child[ch] != null) {
            child[ch].print(strm);
          }
        }
      }
    }

    /**
     * A leaf trie node that does string compares to figure out where the given
     * key belongs between lower..upper.
     */
    static class LeafTrieNode extends TrieNode {
      int lower;
      int upper;
      Text[] splitPoints;
      LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
        super(level);
        this.splitPoints = splitPoints;
        this.lower = lower;
        this.upper = upper;
        //System.out.println("in LeafTrieNode constructor in LeafTrieNode level = "+level+
        //		" splitPoints.length = "+splitPoints.length+" lower = "+lower+" upper = "+upper);
      }
      int findPartition(Text key) {
    	  //System.out.println("\n\nENTERED findPartition 2 in TeraSort.java lower = "+lower+" upper = "+upper+"\n\n");
    	  //for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {System.out.println("ste = "+ste);}
        for(int i=lower; i<upper; ++i) {
          if (splitPoints[i].compareTo(key) > 0) {
        	  //System.out.println("ret3 = "+i);
            return i;
          }
        }
        //System.out.println("ret4 = "+upper);
        return upper;
      }
      void print(PrintStream strm) throws IOException {
        for(int i = 0; i < 2*getLevel(); ++i) {
          strm.print(' ');
        }
        strm.print(lower);
        strm.print(", ");
        strm.println(upper);
      }
    }


    /**
     * Read the cut points from the given sequence file.
     * @param fs the file system
     * @param p the path to read
     * @param job the job config
     * @return the strings to split the partitions on
     * @throws IOException
     */
    private static Text[] readPartitions(FileSystem fs, Path p,
        Configuration conf) throws IOException {
    	
    	
    	
      int reduces = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
      
      if(conf.getInt("mapred.job.bft", 1)==3){reduces=reduces/conf.getInt("mapred.job.numreplicas", 1);}
      
      
      
      //System.out.println("\n\nENTERED readPartitions in TeraSort.java and reduces = "+reduces+"\n\n ");
      
      Text[] result = new Text[reduces - 1];
      DataInputStream reader = fs.open(p);
      for(int i=0; i < reduces - 1; ++i) {
        result[i] = new Text();
        result[i].readFields(reader);
      }
      reader.close();
      return result;
    }

    /**
     * Given a sorted set of cut points, build a trie that will find the correct
     * partition quickly.
     * @param splits the list of cut points
     * @param lower the lower bound of partitions 0..numPartitions-1
     * @param upper the upper bound of partitions 0..numPartitions-1
     * @param prefix the prefix that we have already checked against
     * @param maxDepth the maximum depth we will build a trie for
     * @return the trie node that will divide the splits correctly
     */
    private static TrieNode buildTrie(Text[] splits, int lower, int upper, 
                                      Text prefix, int maxDepth) {
    	//System.out.println("lower = "+lower+" upper = "+upper+" maxDepth = "+maxDepth);
      int depth = prefix.getLength();
      if (depth >= maxDepth || lower == upper) {
    	  //System.out.println("ENETERD lower = "+lower+" upper = "+upper+" depth = "+depth+" maxDepth = "+maxDepth); 
        return new LeafTrieNode(depth, splits, lower, upper);
      }
      InnerTrieNode result = new InnerTrieNode(depth);
      Text trial = new Text(prefix);
      // append an extra byte on to the prefix
      trial.append(new byte[1], 0, 1);
      int currentBound = lower;
      for(int ch = 0; ch < 255; ++ch) {
        trial.getBytes()[depth] = (byte) (ch + 1);
        lower = currentBound;
        while (currentBound < upper) {
          if (splits[currentBound].compareTo(trial) >= 0) {
            break;
          }
          currentBound += 1;
        }
        trial.getBytes()[depth] = (byte) ch;
        result.child[ch] = buildTrie(splits, lower, currentBound, trial, 
                                     maxDepth);
        
        //System.out.println("~~~~INSIDE buildTrie insdie the while splits = "+splits+" lower = "+lower+" currentBound = "
        //+currentBound+" maxDepth = "+maxDepth);
      }
      // pick up the rest
      trial.getBytes()[depth] = (byte) 255;
      result.child[255] = buildTrie(splits, currentBound, upper, trial,
                                    maxDepth);
      
      //System.out.println("++++INSIDE buildTrie outside the while splits = "+splits+" upper = "+upper+" currentBound = "
    	//        +currentBound+" maxDepth = "+maxDepth);
      
      return result;
    }

    public void setConf(Configuration conf) {
      try {
        FileSystem fs = FileSystem.getLocal(conf);
        this.conf = conf;
        Path partFile = new Path(TeraInputFormat.PARTITION_FILENAME);
        splitPoints = readPartitions(fs, partFile, conf);
        //System.out.println("----------- splitPoints.length = "+splitPoints.length);
        trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
      } catch (IOException ie) {
        throw new IllegalArgumentException("can't read partitions file", ie);
      }
    }

    public Configuration getConf() {
      return conf;
    }
    
    public TotalOrderPartitioner() {
    }

    public int getPartition(Text key, Text value, int numPartitions) {
//    	System.out.println("in getPartition in TeraSort.java BFT_FLAG_LOCAL = "+BFT_FLAG_LOCAL);
//    	System.out.println("in getPartition in TeraSort.java Globals.BFT_FLAG_LOCAL_global = "+Globals.BFT_FLAG_LOCAL_global);
//    	System.out.println("in getPartition in TeraSort.java conf.getInt(\"mapred.job.bft\", 1) = "+conf.getInt("mapred.job.bft", 1));
//    	
//    	System.out.println("numPartitions = "+numPartitions);
    	
    	
    	if(conf.getInt("mapred.job.bft", 1)==3)
    	{
    		//System.out.println("numPartitions = "+numPartitions);
    		//numPartitions=numPartitions/conf.getInt("mapred.job.numreplicas", 1);
    		//System.out.println("numPartitions = "+numPartitions);
		}
    	//System.out.println("\n\nENTERED getPartition 1 in TeraSort.java \n\n");
    	int partitionReturnValue=trie.findPartition(key);
    	//System.out.println("partitionReturnValue = "+partitionReturnValue);
    	return partitionReturnValue;
      //return trie.findPartition(key);
    }
    
  }
  
  /**
   * A total order partitioner that assigns keys based on their first 
   * PREFIX_LENGTH bytes, assuming a flat distribution.
   */
  public static class SimplePartitioner extends Partitioner<Text, Text>
      implements Configurable {
    int prefixesPerReduce;
    private static final int PREFIX_LENGTH = 3;
    private Configuration conf = null;
    public void setConf(Configuration conf) {
    	int newNumReduces=conf.getInt(MRJobConfig.NUM_REDUCES, 1);
    	
    	//not needed
    	if(conf.getInt("mapred.job.bft", 1)==3)
    	{
    		//System.out.println("IN setConf");
    		
    		newNumReduces=newNumReduces/conf.getInt("mapred.job.numreplicas", 1);
		}   	
    	
    	
      this.conf = conf;
      prefixesPerReduce = (int) Math.ceil((1 << (8 * PREFIX_LENGTH)) / 
        (float) newNumReduces);
      
      //System.out.println("prefixesPerReduce = "+prefixesPerReduce);
    }
    
    public Configuration getConf() {
      return conf;
    }
    
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
    	//System.out.println("\n\nENTERED getPartition 2 in TeraSort.java \n\n");
    	//if(conf.getInt("mapred.job.bft", 1)==3){numPartitions=numPartitions/conf.getInt("mapred.job.numreplicas", 1);}
      byte[] bytes = key.getBytes();
      int len = Math.min(PREFIX_LENGTH, key.getLength());
      int prefix = 0;
      for(int i=0; i < len; ++i) {
        prefix = (prefix << 8) | (0xff & bytes[i]);
      }
      int retValue = prefix / prefixesPerReduce;
      
      //System.out.println("==== retValue = "+retValue);
      
      return retValue;
    }
  }

  public static boolean getUseSimplePartitioner(JobContext job) {
    return job.getConfiguration().getBoolean(SIMPLE_PARTITIONER, false);
  }

  public static void setUseSimplePartitioner(Job job, boolean value) {
    job.getConfiguration().setBoolean(SIMPLE_PARTITIONER, value);
  }

  public static int getOutputReplication(JobContext job) {
    return job.getConfiguration().getInt(OUTPUT_REPLICATION, 1);
  }

  public static void setOutputReplication(Job job, int value) {
    job.getConfiguration().setInt(OUTPUT_REPLICATION, value);
  }

  public int run(String[] args) throws Exception {
	  //System.out.println("\n\nENTERED run in TeraSort.java\n\n");
	  
	  //System.out.println("------------who calls run in TeraSort.java : --------------");
	  //for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {System.out.println("ste = "+ste);}
	  
    LOG.info("starting");
    
    Job job = Job.getInstance(getConf());
    Path inputDir = new Path(args[0]);
    //Path[] outputDir ;//Path outputDir=new Path(args[1]);
    double test = Math.random() ;//* ( 10 - 1 );
    //Random rand;// = new Random();
    Path outputDir = new Path(args[1]+test);
//    for (int i=0;i<r3;i++)
//    {
//    	outputDir[i]= new Path(args[1]+Integer.toString(i));
//    }
    boolean useSimplePartitioner = getUseSimplePartitioner(job);
    TeraInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    
    //System.out.println("\n\n job.getNumReduceTasks() = "+job.getNumReduceTasks()+"\n\n");
    
    if(job.getConfiguration().getInt("mapred.job.bft", 1)==3)
    {
    	job.setNumReduceTasks(job.getNumReduceTasks()/job.getConfiguration().getInt("mapred.job.numreplicas", 1));
    	
    	//System.out.println("\n\n new job.getNumReduceTasks() = "+job.getNumReduceTasks()+"\n\n");
	}
    
    job.setJobName("TeraSort");
    job.setJarByClass(TeraSort.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TeraInputFormat.class);
	job.setOutputFormatClass(TeraOutputFormat.class);
    
    if (useSimplePartitioner) {
      job.setPartitionerClass(SimplePartitioner.class);
    } else {
      long start = System.currentTimeMillis();
      Path partitionFile = new Path(outputDir, 
                                    TeraInputFormat.PARTITION_FILENAME);
      URI partitionUri = new URI(partitionFile.toString() +
                                 "#" + TeraInputFormat.PARTITION_FILENAME);
      try {
        TeraInputFormat.writePartitionFile(job, partitionFile);
      } catch (Throwable e) {
        LOG.error(e.getMessage());
        return -1;
      }
      job.addCacheFile(partitionUri);  
      long end = System.currentTimeMillis();
      System.out.println("Spent " + (end - start) + "ms computing partitions.");
      job.setPartitionerClass(TotalOrderPartitioner.class);
    }
    
    job.getConfiguration().setInt("dfs.replication", getOutputReplication(job));
    TeraOutputFormat.setFinalSync(job, true);
    //int ret = job.waitForCompletion(true) ? 0 : 1;
    
    
    switch (BFT_FLAG_LOCAL) 
	{
        case 1://No BFT
        {
        	//System.out.println("------in TeraSort.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
        	job.waitForCompletion(true);
        	break;
        }
        case 2://BFT: replicate the AM(it should replicate the mappers and reducers by itself)   //deal with it as No BFT
        {
        	//System.out.println("------in TeraSort.java----job.submit();-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
        	job.submit();
        	break;	        
        }
        case 3://BFT: replicate mappers and reducers (both r times ?), single AM
        {
        	//System.out.println("------in TeraSort.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
        	job.waitForCompletion(true);
        	break;
        }
        case 4://BFT: replicate the AM (r3 times in TeraSort.java) and replicate mappers and reducers (both r times)
        {
        	//Not used
        	break;	        
        }
        default://deal with it as No BFT
        {
        	//System.out.println("------in TeraSort.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL is in default case");
        	job.waitForCompletion(true);
        	break;
        }
	}
    
    
    
    LOG.info("done");
    //return ret;
    return 0;
  }
  
  

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
	  //System.out.println("\n\nENTERED main in TeraSort.java\n\n");
	  
	  
	  //int r3=0;//default//number of AM replicas
	  //int BFT_FLAG_LOCAL = 0;
	  
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
      				//System.out.println(".........name : " + eElement.getElementsByTagName("name").item(0).getTextContent());
      				//System.out.println(".........value : " + eElement.getElementsByTagName("value").item(0).getTextContent());
      				BFT_FLAG_LOCAL=Integer.parseInt(eElement.getElementsByTagName("value").item(0).getTextContent().toString());
      				Globals.BFT_FLAG_LOCAL_global=Integer.parseInt(eElement.getElementsByTagName("value").item(0).getTextContent().toString());
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
	        	//System.out.println("------ENTERED case 1---------");
	        	r3=1;
	        	break;
	        }
	        case 2://BFT: replicate the AM(it should replicate the mappers and reducers by itself)   //deal with it as No BFT
	        {
	        	//System.out.println("------ENTERED case 2---------");
	        	r3=4;
	        	break;	        
	        }
	        case 3://BFT: replicate mappers and reducers (both r times ?), single AM
	        {
	        	//System.out.println("------ENTERED case 3---------");
	        	r3=1;
	        	break;
	        }
	        case 4://BFT: replicate the AM (r3 times in WordCount.java) and replicate mappers and reducers (both r times)
	        {
	        	//System.out.println("------ENTERED case 4---------");
	        	r3=4;
	        	break;	        
	        }
	        default://deal with it as No BFT
	        {
	        	//System.out.println("------ENTERED default---------");
	        	r3=1;
	        	break;
	        }
		}
	    
	    
	    Configuration[] conf = new Configuration[r3];
	    for( int i=0; i<r3; i++ )
	    {
	    	conf[i] = new Configuration();
//	    	if(conf[i].getInt("mapred.job.bft", 1)==3)
//	    	{
//	    		System.out.println("IN setConf");
//	    		
//	    		//conf[i]=newNumReduces/conf.getInt("mapred.job.numreplicas", 1);
//			}   	
	   
	    	ToolRunner.run(conf[i], new TeraSort(), args);
	    		
	    }
	    	
	    
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
    //int res = ToolRunner.run(new Configuration(), new TeraSort(), args);
	    
    //System.exit(res);
	    System.exit(0);
  }

}
