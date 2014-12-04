package org.apache.hadoop.examples;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.MedianWeather.MedianWeatherMapper;
import org.apache.hadoop.examples.MedianWeather.MedianWeatherReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
public class Dictionary
{
    public static class WordMapper extends Mapper<Text, Text, Text, Text>
    {
        private Text word = new Text();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(key, word);
            }
        }
    }
    public static class AllTranslationsReducer
    extends Reducer<Text,Text,Text,Text>
    {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException
        {
            String translations = "";
            for (Text val : values)
            {
                translations += "|"+val.toString();
            }
            result.set(translations);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception
    {
    	
    	
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
		
		
		//Configuration conf = new Configuration();	
		
		String[] otherArgs = new GenericOptionsParser(conf[0], args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: dictionary <in> <out>");
		  System.exit(2);
		}
		
		
		for (int i=0;i<r3;i++)
		  {
			
//			// Set the outputs for the Map
//	        conf[i].setMapOutputKeyClass(Text.class);
//	        conf.setMapOutputValueClass(IntWritable.class);
//
//	        // Set the outputs for the Job
//	        conf.setOutputKeyClass(Text.class);
//	        conf.setOutputValueClass(ArrayWritable.class);
//
			
			 System.out.println("------INSIDE the for loop , r3 = --------- "+r3+" -------------- ");
			  
			  Job job = new Job(conf[i], "dictionary");
			  
			  System.out.println("job.getJobID() = "+job.getJobID()+" job.getJobName() = "+job.getJobName());
			  
			  job.setJarByClass(Dictionary.class);
		        job.setMapperClass(WordMapper.class);
		        job.setReducerClass(AllTranslationsReducer.class);
		        
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
		        
		        job.setMapOutputKeyClass(Text.class);
		        job.setMapOutputValueClass(Text.class);
		        
		        job.setInputFormatClass(KeyValueTextInputFormat.class);
		        job.setOutputFormatClass(TextOutputFormat.class);
		        
		        
		        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//"/tmp/hadoop-cscarioni/dfs/name/file"
		        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//"output"
				
				
				switch (BFT_FLAG_LOCAL) 
				{
			        case 1://No BFT
			        {
			        	System.out.println("------in Dictionary.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
			        	job.waitForCompletion(true);
			        	break;
			        }
			        case 2://BFT: replicate the AM(it should replicate the mappers and reducers by itself)   //deal with it as No BFT
			        {
			        	System.out.println("------in Dictionary.java----job.submit();-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
			        	job.submit();
			        	break;	        
			        }
			        case 3://BFT: replicate mappers and reducers (both r times ?), single AM
			        {
			        	System.out.println("------in Dictionary.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL  = "+BFT_FLAG_LOCAL);
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
			        	System.out.println("------in Dictionary.java----job.waitForCompletion(true);-----cuz BFT_FLAG_LOCAL is in default case");
			        	job.waitForCompletion(true);
			        	break;
			        }
				}
				
				//System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		  }
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
//    	
//        //Configuration conf = new Configuration();
//        //Job job = new Job(conf, "dictionary");
//        job.setJarByClass(Dictionary.class);
//        job.setMapperClass(WordMapper.class);
//        job.setReducerClass(AllTranslationsReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//"/tmp/hadoop-cscarioni/dfs/name/file"
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//"output"
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}








