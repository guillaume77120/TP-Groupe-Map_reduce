package com.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MonDriver is a Hadoop driver class that configures and runs a MapReduce job.
 * It extends the Configured class and implements the Tool interface.
 * 
 * The run method sets up the job configuration, including the mapper and reducer classes,
 * input and output formats, and the number of reduce tasks. It then starts the job and waits
 * for its completion.
 * 
 * The main method initializes the Hadoop configuration and runs the MonDriver class using
 * the ToolRunner.
 * 
 * @throws Exception if an error occurs during job execution
 */
public class MonDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance();
		
		job.setJarByClass(MonMapper.class);
		job.setJobName("monPremierMapReduce");
		
		job.setMapperClass(MonMapper.class);
		job.setReducerClass(MonReducer.class);
		job.setNumReduceTasks(10);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/mpmr/input"));
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("/mpmr/output"));
		
		job.waitForCompletion(true);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		System.exit(ToolRunner.run(conf, new MonDriver(), args));
	}

}

