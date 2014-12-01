package org.apache.hadoop.examples;



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.MedianWeather.MedianWeatherMapper;
import org.apache.hadoop.examples.MedianWeather.MedianWeatherReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.TextOutputFormat;





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
 
public class WikiGeo {
	
	public static class WikiGeoMapper extends //extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

public static String GEO_RSS_URI = "http://www.georss.org/georss/point";

private Text geoLocationKey = new Text();
private Text geoLocationName = new Text();


public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> outputCollector, Reporter reporter)
            throws IOException {

    String dataRow = value.toString();

    // since these are tab seperated files lets tokenize on tab
    StringTokenizer dataTokenizer = new StringTokenizer(dataRow, "\t");
    String articleName = dataTokenizer.nextToken();
    String pointType = dataTokenizer.nextToken();
    String geoPoint = dataTokenizer.nextToken();
    // we know that this data row is a GEO RSS type point.
    if (GEO_RSS_URI.equals(pointType)) {
            // now we process the GEO point data.
            StringTokenizer st = new StringTokenizer(geoPoint, " ");
            String strLat = st.nextToken();
            String strLong = st.nextToken();
            double lat = Double.parseDouble(strLat);
            double lang = Double.parseDouble(strLong);
            long roundedLat = Math.round(lat);
            long roundedLong = Math.round(lang);
            String locationKey = "(" + String.valueOf(roundedLat) + ","
                            + String.valueOf(roundedLong) + ")";
            String locationName = URLDecoder.decode(articleName, "UTF-8");
            locationName = locationName.replace("_", " ");
            locationName = locationName + ":(" + lat + "," + lang + ")";
            geoLocationKey.set(locationKey);
            geoLocationName.set(locationName);
            outputCollector.collect(geoLocationKey, geoLocationName);
    }

}

}

	
	public static class WikiGeoReducer extends //extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

private Text outputKey = new Text();
private Text outputValue = new Text();


public void reduce(Text geoLocationKey, Iterator<Text> geoLocationValues,
            OutputCollector<Text, Text> results, Reporter reporter)
            throws IOException {
    // in this case the reducer just creates a list so that the data can
    // used later
    String outputText = "";
    while (geoLocationValues.hasNext()) {
            Text locationName = geoLocationValues.next();
            outputText = outputText + locationName.toString() + " ,";
    }
    outputKey.set(geoLocationKey.toString());
    outputValue.set(outputText);
    results.collect(outputKey, outputValue);
}

}
	
	
	public static void main(String[] args) throws Exception {
        
		Configuration conf = new Configuration();
		
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		  System.err.println("Usage: wikigeo <in> <out>");
		  System.exit(2);
		}
		
		Job job = new Job(conf, "WikiGeo");
		
		job.setJarByClass(WikiGeo.class);
		job.setMapperClass(WikiGeoMapper.class);
		//job.setCombinerClass(MedianWeatherReducer.class);
		job.setReducerClass(WikiGeoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(MedianWeather.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		


}

	
	
	
	
	
	
	
}
