

/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 2 *********************
  *****************************************
  *****************************************
  */

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/data/shakespeare"; 
		String temp = "/lab2/shakespeare/temp";
		String output = "/lab2/shakespeare/output"; 

		// The number of reduce tasks 
		int reduce_tasks = 2; 
		
		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Driver Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Driver.class);

		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);

		
		// The datatype of the mapper output Key, Value
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(IntWritable.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(IntWritable.class);

		// The class that provides the map method
		job_one.setMapperClass(Map_One.class);

		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input));
		
		// This is legal
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path));
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		
		// This is not allowed
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); 

		// Run the job
		job_one.waitForCompletion(true);

		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1

		Job job_two = Job.getInstance(conf, "Driver Program Round Two");
		job_two.setJarByClass(Driver.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(IntWritable.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		// Run the job
		job_two.waitForCompletion(true);

		/**
		 * **************************************
		 * ************************************** 
		 * FILL IN CODE FOR MORE JOBS IF YOU NEED 
		 * **************************************
		 * **************************************
		 */

	}

	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text
	// (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can
	// be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and
	// IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			/**
			 * ***********************************
			 * ***********************************
			 * FILL IN CODE FOR THE MAP FUNCTION
			 * ***********************************
			 * ***********************************
			 */
			
			//Convert each line to string format
            String line = value.toString();
            // Convert all the letters to lower case as mentioned in Q.
            line = line.toLowerCase();

            // Replace all special characters except "space" and ". by ""
            line = line.replaceAll("[^a-z0-9. ]","");
            // For guttenberg dataset, to take care of '.' they had to be appended with spaces.
            line = line.replaceAll("[.]", " . ");

            StringTokenizer tokenizer = new StringTokenizer(line);

            //Set prev string as "."
            String prev = ".";

            while(tokenizer.hasMoreTokens()){

                String current = tokenizer.nextToken();

				// If either prev or current contains "." means that it is end of sentence
				//Should not be included as bigram.
                if(!prev.contains(".") && !current.contains(".")) {
                	word.set(prev + " " + current);
                	context.write(word, one);
                }
                    
                prev = current;
            }
			
			


			// Use context.write to emit values
		} 
	} 

	
	// The Reduce class
	// The key is Text and must match the datatype of the output key of the map
	// method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable> {

		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//
			/**
			 * **************************************
			 * **************************************
			 * YOUR CODE HERE FOR THE REDUCE FUNCTION
			 * **************************************
			 * **************************************
			 */


			// Find the count of each bigram sent from the mapper

			int sum = 0;
			for (IntWritable val : values) {

				int value = val.get();
				sum += value;


				
			}



			// Use context.write to emit values as (bigram key , 1) etc.
			context.write(key, new IntWritable(sum));

		} 
	}

	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text> {
		
		private final static Text bigramKey = new Text("one");

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			/**
			 * ***********************************
			 * *********************************** 
			 * FILL IN CODE FOR THE MAP FUNCTION 
			 * ***********************************
			 * ***********************************
			 */
			// The output of the previous reducer needs to be combined into one key, so that
			// the reducer at the next stage can process each bigram count and determine top 10
			context.write(bigramKey, value);

		} 
	} 

	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			/**
			 * **************************************
			 * ************************************** 
			 * YOUR CODE HERE FOR THE REDUCE FUNCTION 
			 * **************************************
			 * **************************************
			 */
			
			ArrayList<String> stringArrayList = new ArrayList<String>();
			
			for(Text t : values) {
				stringArrayList.add(t.toString());
			}

			// Storing all bigrams and their counts in a hash-map
			final HashMap<String,Integer> map = new HashMap<String,Integer>();

			for(String s: stringArrayList){
			    String [] biGram = s.split("\t");
			    map.put(biGram[0], Integer.parseInt(biGram[1]));
			}
			// Using a comparator to sort the hashmap, based on count
			Comparator<String> comparator = new Comparator<String>() {
			    public int compare(String o1, String o2) {
			        return map.get(o1).compareTo(map.get(o2));
			    }
			};

			ArrayList<String> words = new ArrayList<String>();
			words.addAll(map.keySet());

			Collections.sort(words,comparator);
			//Reversing to get correct order from max to least
			Collections.reverse(words);

			// Pulling out the top 10 bigrams.
			for (int i = 0; i < 10; i++)
			{
				
				Text text = new Text(words.get(i).toString());
				IntWritable value = new IntWritable(map.get(words.get(i)));
				
				context.write(text, value);
				
			}
						

		} 
	} 

	/**
	 * ******************************************************
	 * ****************************************************** 
	 * YOUR CODE HERE FOR MORE MAP / REDUCE CLASSES IF NEEDED
	 * ******************************************************
	 * ******************************************************
	 */

}