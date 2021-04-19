//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Random;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileSystem;
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


public class customSort {
	
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
		
		job.setJarByClass(customSort.class);
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
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("/lab3/exp2/temp/"));
		
		job.waitForCompletion(true);
		
		
		System.out.println("mr1 done!");
		
		//Read the partioner info and set configuration
		//This conf will be used in the second MR stage.
        Path pt = new Path("/lab3/exp2/temp/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        int bucket = 0;
        while (line != null){
        	conf.set(String.valueOf(bucket), line);
            line=br.readLine();
            bucket += 1;
        }
        
        //Delete temporary directory since conf has paritition info
        pt = new Path("/lab3/exp2/temp/");
        if(fs.exists(pt)) {
        	fs.delete(pt, true);
        }
		
		
		
		
		///////////////////////////////////////////////////
		///////////// Second Round MapReduce //////////////
		///////////////////////////////////////////////////
		Job job_two = Job.getInstance(conf, "Round Two");
        job_two.setJarByClass(customSort.class);
        
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

		FileInputFormat.addInputPath(job_two, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job_two, new Path(otherArgs[1]));
         
        // Run the job
 		System.exit(job_two.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static class mapOne extends Mapper<Text, Text, IntWritable, Text> {

		private static IntWritable index = new IntWritable(1);
		private static Text newValue = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			
			newValue.set(key.toString() + " " + value.toString());
			context.write(index,newValue);
		}
	}
	
	public static class reduceOne extends Reducer<IntWritable, Text, Text, NullWritable> {

		private Text word = new Text();
		private static int count = 0;
		private final int NUM_SAMPLES = 50000;

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<String> samples = new ArrayList<String>();
			
			for(Text val:values)
			{
				
				//Randomly choose samples based on coin toss
				double toss = Math.random();
				if(toss < 0.5 && count <= NUM_SAMPLES) {
					String k = val.toString().substring(0,15);
					samples.add(k);
					count += 1;
				}
				
				//Done with sample collection
				if(count > NUM_SAMPLES) {
					break;
				}
			}
			
			//sort samples
			Collections.sort(samples);
			
			
			//Write to temp for parition usage in MR 2 
			int factor = (int)NUM_SAMPLES/10;
			for(int i = factor; i < NUM_SAMPLES; i+=factor) {
				word.set(samples.get(i));
				context.write(word, null);
			}
			

        }
		

	} 
	
	// Compare each input key with the boundaries we get from the first round
	// And add the partitioner information in the end of values
	public static class mapTwo extends Mapper<Text, Text, Text, Text> {
		
		private Configuration conf;
		
		@Override
		public void setup(Context context) {
			conf = context.getConfiguration();
		}
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			// For instance, partitionFile0 to partitionFile8 are the discovered boundaries, 
			// based on which you add the No ID 0 to 9 at the end of value
			// How to find the boundaries is your job for this experiment
			
			
			//Retrieve  boundary information from configuration.
			String tmp4Com = key.toString();
			String partitionFile0 = conf.get("0");
			String partitionFile1 = conf.get("1");
			String partitionFile2 = conf.get("2");
			String partitionFile3 = conf.get("3");
			String partitionFile4 = conf.get("4");
			String partitionFile5 = conf.get("5");
			String partitionFile6 = conf.get("6");
			String partitionFile7 = conf.get("7");
			String partitionFile8 = conf.get("8");

			if (tmp4Com.compareTo(partitionFile0)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(0)));
			} 
			else if(tmp4Com.compareTo(partitionFile1)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(1)));
			}
			else if(tmp4Com.compareTo(partitionFile2)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(2)));
			}
			else if(tmp4Com.compareTo(partitionFile3)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(3)));
			}
			else if(tmp4Com.compareTo(partitionFile4)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(4)));
			}
			else if(tmp4Com.compareTo(partitionFile5)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(5)));
			}
			else if(tmp4Com.compareTo(partitionFile6)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(6)));
			}
			else if(tmp4Com.compareTo(partitionFile7)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(7)));
			}
			else if(tmp4Com.compareTo(partitionFile8)<=0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(8)));
			}
			else if(tmp4Com.compareTo(partitionFile8) >0) {
				context.write(new Text(key), new Text(value + ";" + Integer.toString(9)));
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