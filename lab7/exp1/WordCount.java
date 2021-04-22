import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("serial")
public class WordCount {
        
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        // <PATH_TO_DATA>: The path to input data, e.g., "/home/cpre419/Downloads/shakespeare"
        DataStream<String> text = env.readTextFile("/home/cpre419/Downloads/shakespeare");

        DataStream<Tuple2<String, Integer>> counts =
              // split up the lines in pairs (2-tuples) containing: (word,1)
              text.flatMap(new Tokenizer());
              // group by the tuple field "0" and sum up tuple field "1"
//              .keyBy(0);
//              .sum(1);


        //Adding a sink function to group by key and count
        counts.addSink(new CustomSinkFunction());
                    
 
         env.execute("Streaming WordCount Example");
    }  
    
    
    public static final class CustomSinkFunction extends RichSinkFunction<Tuple2<String, Integer>> {

        HashMap <String,Integer> countWord = new HashMap<String,Integer>();

        // If key exists, add 1 to count, else add key to hashmap
        public void invoke(Tuple2<String, Integer> value) throws Exception {
            if (countWord.get(value.f0) != null)
            { countWord.put(value.f0, countWord.get(value.f0)+1);}
            else
            {countWord.put(value.f0, 1);}
        }

        @Override
        public void close() throws IOException { 
//            System.out.println("----------------------------------------");
//            System.out.println(Arrays.asList(countWord));      
            printTopK(10);
            
        }
        //comparator to sort by decreasing order
        public void printTopK(int k) {
            
 			Comparator<String> comparator = new Comparator<String>() {
 			    public int compare(String o1, String o2) {
 			        return countWord.get(o1).compareTo(countWord.get(o2));
 			    }
 			};

 			ArrayList<String> words = new ArrayList<String>();
 			words.addAll(countWord.keySet());

 			Collections.sort(words,comparator);
 			Collections.reverse(words);
 			
 			System.out.println("Top " + k + " words");
             System.out.println("----------------------------------------");
//             System.out.println(Arrays.asList(words));
             
             for(int i = 0; i < k; i++) {
             	System.out.println(words.get(i) + " : " + countWord.get(words.get(i)));
             }
             
        }
    }

    
    
    

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
        
            String[] tokens = value.toLowerCase().split("\\W+");
            
            System.out.println(tokens);

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}