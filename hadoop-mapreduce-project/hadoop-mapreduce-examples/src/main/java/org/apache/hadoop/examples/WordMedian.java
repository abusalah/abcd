package org.apache.hadoop.examples;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.common.base.Charsets;

public class WordMedian extends Configured implements Tool {

  private double median = 0;
  private final static IntWritable ONE = new IntWritable(1);

  /**
   * Maps words from line of text into a key-value pair; the length of the word
   * as the key, and 1 as the value.
   */
  public static class WordMedianMapper extends
      Mapper<Object, Text, IntWritable, IntWritable> {

    private IntWritable length = new IntWritable();

    /**
     * Emits a key-value pair for counting the word. Outputs are (IntWritable,
     * IntWritable).
     * 
     * @param value
     *          This will be a line of text coming in from our input file.
     */
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String string = itr.nextToken();
        length.set(string.length());
        context.write(length, ONE);
      }
    }
  }

  /**
   * Performs integer summation of all the values for each key.
   */
  public static class WordMedianReducer extends
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable val = new IntWritable();

    /**
     * Sums all the individual values within the iterator and writes them to the
     * same key.
     * 
     * @param key
     *          This will be a length of a word that was read.
     * @param values
     *          This will be an iterator of all the values associated with that
     *          key.
     */
    public void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      val.set(sum);
      context.write(key, val);
    }
  }

  /**
   * This is a standard program to read and find a median value based on a file
   * of word counts such as: 1 456, 2 132, 3 56... Where the first values are
   * the word lengths and the following values are the number of times that
   * words of that length appear.
   * 
   * @param path
   *          The path to read the HDFS file from (part-r-00000...00001...etc).
   * @param medianIndex1
   *          The first length value to look for.
   * @param medianIndex2
   *          The second length value to look for (will be the same as the first
   *          if there are an even number of words total).
   * @throws IOException
   *           If file cannot be found, we throw an exception.
   * */
  private double readAndFindMedian(String path, int medianIndex1,
      int medianIndex2, Configuration conf) throws IOException {
	  
	  System.out.println("----_0");
	  
    FileSystem fs = FileSystem.get(conf);
    Path file = new Path(path, "part-r-00000"); 
    
    System.out.println("----_1");

    if (!fs.exists(file))
      throw new IOException("Output not found!");

    System.out.println("----_2");
    
    BufferedReader br = null;

    try {
    	
    	System.out.println("----_3");
    	
      br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
      int num = 0;
      
  	 System.out.println("----_4");

   
      String line;  
      while ((line = br.readLine()) != null) {
    	  
      	System.out.println("----_5");

        StringTokenizer st = new StringTokenizer(line);

        // grab length
        String currLen = st.nextToken();

        // grab count
        String lengthFreq = st.nextToken();

        int prevNum = num;
        num += Integer.parseInt(lengthFreq);

    	System.out.println("----_5");

        
        if (medianIndex2 >= prevNum && medianIndex1 <= num) {
        	System.out.println("----_6 1");

          System.out.println("The median is: " + currLen);
          br.close();
          return Double.parseDouble(currLen);
        } else if (medianIndex2 >= prevNum && medianIndex1 < num) {
        	System.out.println("----_6 2");

          String nextCurrLen = st.nextToken();
          double theMedian = (Integer.parseInt(currLen) + Integer
              .parseInt(nextCurrLen)) / 2.0;
          System.out.println("The median is: " + theMedian);
          br.close();
          return theMedian;
        }
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    // error, no median found
    return -1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new WordMedian(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: wordmedian <in> <out>");
      return 0;
    }
    
    long totalWords = 0;
    int medianIndex1;
    int medianIndex2;
    
    
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
	    

    //setConf(new Configuration());//WAS IN THE ORIGINAL CODE
    //Configuration conf = getConf();//WAS IN THE ORIGINAL CODE
	     
	     Configuration[] conf = new Configuration[r3];
		    for( int i=0; i<r3; i++ )
		    {
		    	conf[i] = new Configuration();
		    		
		    
		    
		    System.out.println("------INSIDE the for loop , r3 = --------- "+r3+" -------------- ");
		    	


    @SuppressWarnings("deprecation")
    Job job = new Job(conf[i], "word median");
    
	  System.out.println("job.getJobID() = "+job.getJobID()+" job.getJobName() = "+job.getJobName());

    
    job.setJarByClass(WordMedian.class);
    job.setMapperClass(WordMedianMapper.class);
    job.setCombinerClass(WordMedianReducer.class);
    job.setReducerClass(WordMedianReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //boolean result = job.waitForCompletion(true);
    
    switch (BFT_FLAG_LOCAL) 
	{
        case 1://No BFT
        {
        	System.out.println("------in WordMedian.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
        	job.waitForCompletion(true);
        	break;
        }
        case 2://BFT: replicate the AM(it should replicate the mappers and reducers by itself)   //deal with it as No BFT
        {
        	System.out.println("------in WordMedian.java----job.submit();-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
        	job.submit();
        	break;	        
        }
        case 3://BFT: replicate mappers and reducers (both r times ?), single AM
        {
        	System.out.println("------in WordMedian.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
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
        	System.out.println("------in WordMedian.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL is in default case");
        	job.waitForCompletion(true);
        	break;
        }
	}
    
 // Wait for JOB 1 -- get middle value to check for Median

    totalWords = job.getCounters()
       .getGroup(TaskCounter.class.getCanonicalName())
       .findCounter("MAP_OUTPUT_RECORDS", "Map output records").getValue();
    
   
   
    medianIndex1 = (int) Math.ceil((totalWords / 2.0));
    medianIndex2 = (int) Math.floor((totalWords / 2.0));
    

   median = readAndFindMedian(args[1], medianIndex1, medianIndex2, conf[0]);

   System.out.println("my median = "+median + " totalWords = "+totalWords + " medianIndex1 = "+medianIndex1 +" medianIndex2 = "+medianIndex2 );

    
    
		    }

		 
		    
		   

		    		   
    

    return 0;//(result ? 0 : 1);
  }

  public double getMedian() {
    return median;
  }
}
