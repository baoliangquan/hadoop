package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author duanhaitao@itcast.cn
 *
 */
//com.hadoop.execu.mapreduce.WordCountRunner
public class WordCountRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job wcjob = Job.getInstance(conf);
		//conf.set("mapreduce.job.jar", "wcount.jar");
		
		wcjob.setJarByClass(WordCountRunner.class);

		wcjob.setMapperClass(WordCountMapper.class);
		wcjob.setReducerClass(WordCountReducer.class);
		
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(wcjob, "hdfs://ns1/mydata/wordcount");
		//FileInputFormat.setInputPaths(wcjob, "F:\\bbbbb.txt");

		FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://ns1/mydata/shutout"));
		//FileOutputFormat.setOutputPath(wcjob, new Path("F:\\bbbbb"));

		boolean res = wcjob.waitForCompletion(true);
		
		System.exit(res?0:1);
		
		
	}
	
	
	
}
