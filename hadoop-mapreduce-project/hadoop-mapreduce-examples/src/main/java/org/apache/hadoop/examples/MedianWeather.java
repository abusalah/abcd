package org.apache.hadoop.examples;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.VoteCountApplication;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.examples.VoteCountApplication.VoteCountMapper;
import org.apache.hadoop.examples.VoteCountApplication.VoteCountReducer;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
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
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;







import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MedianWeather {
	
	public static class MedianWeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		Text day = new Text();
		Text outVal = new Text();
		FloatWritable temp = new FloatWritable();
	  
	  	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	  		
	  		try {
				String record = value.toString();
				
//	  			String dayPart = record.substring(14, 22);
//	  			String tempePart = record.substring(24,30);
	  			
//	  			if(tempePart.trim().equalsIgnoreCase("TEMP"))
//	  				return; //Invalid record
	  			String[] data = record.split("\\s+");
	  			//dayPart - 2
	  			//temp - 3
	  			//dew point - 4
	  			//
//	  			String dewPoint = record.substring(37,30);
	  			
	  			day.set(data[2]);
	  			outVal.set(data[3]+"\t"+data[4]);
//	  			int yearInt = Integer.parseInt(yearPart);
//	  			System.out.println("2(key) = " +data[2]+"3="+data[3]+"dew 4 "+data[4]);
	  			context.write(day,outVal);
			} catch (Exception e) {
				e.printStackTrace();
				
			}
	  		
	  	}
	  	
	} 
	
	
	
	
	public static class MedianWeatherReducer extends Reducer<Text, Text, Text, Text> {

		final int SAMPLE_SIZE = 1000;
		FloatWritable outputTemp = new FloatWritable();
		Text outputValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// FloatWritable median = findMedian(values);
			long count = 0;
			double tempavg = 0.0, dewpointavg = 0.0;
			for (Text val : values) {
				count++;
				String valArrp[] = val.toString().split("\t");
				try {
					tempavg += Double.parseDouble(valArrp[0]);
					dewpointavg += Double.parseDouble(valArrp[1]);
				} catch (NumberFormatException nfe) {
					;
				}
			}
			tempavg = tempavg / count;
			dewpointavg = dewpointavg / count;
			outputValue.set(tempavg + "\t" + dewpointavg);
			context.write(key, outputValue);
		}

		private FloatWritable findMedian(Iterable<FloatWritable> values) {
			List<FloatWritable> sortedValues = sort(values);
			int medianPos = sortedValues.size() / 2 + 1;
			return sortedValues.get(medianPos - 1);
		}

		private List<FloatWritable> sort(Iterable<FloatWritable> values) {

			List<FloatWritable> list = new ArrayList<FloatWritable>();

			// SAMPLED SORTING - Not considering more than this amount of records
			// for each year.
			int count = 0;
			for (FloatWritable val : values) {
				list.add(val);
				count++;
				// if(count>=SAMPLE_SIZE)
				// break;
			}

			int size = list.size();
			for (int i = 0; i < size; i++) {

				FloatWritable min = null;

				for (int j = i; j < size; j++) {
					if (min == null) {
						min = list.get(j);
					} else {
						if (list.get(j).get() < min.get()) {
							min = list.get(j);
						}
					}

				}

				// swapping
				float temp = min.get();
				min.set(list.get(i).get());
				list.get(i).set(temp);
			}

			return list;
		}

		// public static void main(String[] args) {
		//
		// //testing
		// List<IntWritable> list = new ArrayList<IntWritable>();
		//
		// list.add(new IntWritable(100));
		// list.add(new IntWritable(20));
		// list.add(new IntWritable(100));
		// list.add(new IntWritable(60));
		// list.add(new IntWritable(-5));
		// list.add(new IntWritable(0));
		// list.add(new IntWritable(34));
		// list.add(new IntWritable(-9));
		// list.add(new IntWritable(87));
		// list.add(new IntWritable(43));
		//
		// List<IntWritable> sortedList = sort(list);
		//
		// for(int i=0;i<sortedList.size();i++) {
		// System.out.println(sortedList.get(i).get());
		// }
		//
		// int medianPos = sortedList.size()/2 + 1;
		// System.out.println("Median :" + sortedList.get(medianPos-1).get()) ; ;
		// }
	}



	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();	
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: medianweather <in> <out>");
		  System.exit(2);
		}
		
		Job job = new Job(conf, "MedianWeather");
		
		job.setJarByClass(MedianWeather.class);
		job.setMapperClass(MedianWeatherMapper.class);
		//job.setCombinerClass(MedianWeatherReducer.class);
		job.setReducerClass(MedianWeatherReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setInputFormatClass(TextInputFormat.class);
//      job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(MedianWeather.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		//return 0;
		
		//int res = ToolRunner.run(new Configuration(), new MedianWeather(), args);
        //System.exit(res);
		
		
        
        }//end of main
	
//	@Override
//    public int run(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.out.println("usage: [input] [output]");
//            System.exit(-1);
//        }
//
//        Job job = Job.getInstance(new Configuration());
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        job.setMapperClass(VoteCountMapper.class);
//        job.setReducerClass(VoteCountReducer.class);
//
//        //job.setInputFormatClass(TextInputFormat.class);
//        //job.setOutputFormatClass(TextOutputFormat.class);
//        
//        //job.setOutputKeyClass(Text.class);
//        //job.setOutputValueClass(Text.class);
//
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.setJarByClass(VoteCountApplication.class);
//
//        //job.submit();
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//        return 0;
//    }

}//end of the whole class












//
//
//
//Configuration[] conf = new Configuration[r3];
//for( int i=0; i<r3; i++ )
//{
//	conf[i] = new Configuration();
//		
//}
//	
//
//String[] otherArgs = new GenericOptionsParser(conf[0], args).getRemainingArgs();
//if (otherArgs.length != 2) {
//  System.err.println("Usage: wordcount <in> <out>");
//  System.exit(2);
//}
//
//
//
////long startTime = System.currentTimeMillis()/1000;
////long elapsedTime = 0L;
//		
//for (int i=0;i<r3;i++)
//{
//  System.out.println("------INSIDE the for loop , r3 = --------- "+r3+" -------------- ");
//  
//  Job job = new Job(conf[i], "word count");
//  
//  System.out.println("job.getJobID() = "+job.getJobID()+" job.getJobName() = "+job.getJobName());
//  
//  
//  
//  
//job.setJarByClass(WordCount.class);
//job.setMapperClass(TokenizerMapper.class);
//job.setCombinerClass(IntSumReducer.class);
//job.setReducerClass(IntSumReducer.class);
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(IntWritable.class);
//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+Integer.toString(i)));
////System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//
