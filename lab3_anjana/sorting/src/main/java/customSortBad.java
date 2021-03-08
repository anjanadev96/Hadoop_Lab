//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Random;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class customSortBad {
	
	public static void main(String[] args) throws Exception{ 
	
		///////////////////////////////////////////////////
		///////////// First Round MapReduce ///////////////
		////// where you might want to do some sampling ///
		///////////////////////////////////////////////////
		int reduceNumber = 1;
		
		Configuration conf = new Configuration();		
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: Patent <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Exp2");
		
		job.setJarByClass(customSortBad.class);
		job.setNumReduceTasks(reduceNumber);
	
		job.setMapperClass(mapOne.class);
//		job.setCombinerClass(combinerOne.class);
		job.setReducerClass(reduceOne.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path("/lab3/input-5k/"));

		FileOutputFormat.setOutputPath(job, new Path("/lab3/exp2/temp/"));
		
		job.waitForCompletion(true);
		
		
		
		///////////////////////////////////////////////////
		///////////// Second Round MapReduce //////////////
		///////////////////////////////////////////////////
		Job job_two = Job.getInstance(conf, "Round Two");
        job_two.setJarByClass(customSortBad.class);
        
        conf.setInt("Count", 0);
        // Providing the number of reducers for the second round
        reduceNumber = 10;
        job_two.setNumReduceTasks(reduceNumber);

        // Should be match with the output datatype of mapper and reducer
        job_two.setMapOutputKeyClass(Text.class);
        job_two.setMapOutputValueClass(Text.class);
         
        job_two.setOutputKeyClass(Text.class);
        job_two.setOutputValueClass(Text.class);
        //job_two.setPartitionerClass(MyPartitioner.class);
         
        job_two.setMapperClass(mapTwo.class);
        job_two.setReducerClass(reduceTwo.class);
        
        
        // Partitioner is our custom partitioner class
        job_two.setPartitionerClass(MyPartitioner.class);
        
        // Input and output format class
        job_two.setInputFormatClass(KeyValueTextInputFormat.class);
        job_two.setOutputFormatClass(TextOutputFormat.class);
         
        // The output of previous job set as input of the next
        FileInputFormat.addInputPath(job_two, new Path("/lab3/exp2/temp/"));
        FileOutputFormat.setOutputPath(job_two, new Path("/lab3/exp2/output/"));
         
        // Run the job
 		System.exit(job_two.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static class mapOne extends Mapper<Text, Text, IntWritable, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//			System.out.println("map1 key:" + key);
//			System.out.println("map1 value:"+value);
			Text newValue = new Text();
			newValue.set(key.toString() + " " + value.toString());
			context.write(new IntWritable(1),newValue);
		}
	}
	
	public static class reduceOne extends Reducer<IntWritable, Text, Text, NullWritable> {

		private Text word = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for(Text val:values)
			{
				word.set(val.toString());
				context.write(word, null);
			}
        }
	} 
	
	// Compare each input key with the boundaries we get from the first round
	// And add the partitioner information in the end of values
	public static class mapTwo extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			// For instance, partitionFile0 to partitionFile8 are the discovered boundaries, 
			// based on which you add the No ID 0 to 9 at the end of value
			// How to find the boundaries is your job for this experiment
			
			String[] lineTwo = key.toString().split(" ");
			System.out.println("map2 key:" + key);
			System.out.println("map2 value:"+value);
			Character tmp4Com = key.toString().charAt(0);
			Character partitionFile0 = '6';
			Character partitionFile1 = 'C';
			Character partitionFile2 = 'K';
			Character partitionFile3 = 'Q';
			Character partitionFile4 = 'W';
			Character partitionFile5 = 'c';
			Character partitionFile6 = 'h';
			Character partitionFile7 = 'm';
			Character partitionFile8 = 's';
			
			
			if (tmp4Com.compareTo(partitionFile0)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(0)));
			} 
			else if(tmp4Com.compareTo(partitionFile1)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(1)));
			}
			else if(tmp4Com.compareTo(partitionFile2)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(2)));
			}
			else if(tmp4Com.compareTo(partitionFile3)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(3)));
			}
			else if(tmp4Com.compareTo(partitionFile4)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(4)));
			}
			else if(tmp4Com.compareTo(partitionFile5)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(5)));
			}
			else if(tmp4Com.compareTo(partitionFile6)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(6)));
			}
			else if(tmp4Com.compareTo(partitionFile7)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(7)));
			}
			else if(tmp4Com.compareTo(partitionFile8)<=0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(8)));
			}
			else if(tmp4Com.compareTo(partitionFile8) >0) {
				context.write(new Text(lineTwo[0]), new Text(lineTwo[1] + ";" + Integer.toString(9)));
			}
				
		}
	}
	
	public static class reduceTwo extends Reducer<Text, Text, Text, Text> {
		private Text word = new Text();
     	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			for(Text val:values)
			{
				String[] desTmp = val.toString().split(";");
				word.set(desTmp[0].toString());
				context.write(key, word);
			}
     	}
	 }
	
	// Extract the partitioner information from the input values, which decides the destination of data
	public static class MyPartitioner extends Partitioner<Text,Text>{
		
	   public int getPartition(Text key, Text value, int numReduceTasks){
		   
		   String[] desTmp = value.toString().split(";");
		   return Integer.parseInt(desTmp[1]);
		   
	   }
	}
}