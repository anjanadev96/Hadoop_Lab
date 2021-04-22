
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;


import java.io.PrintStream;
import java.io.File;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;


@SuppressWarnings("serial")
public class WordCountWindow {
        
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        
        //set output data path
        File file = new File("/home/cpre419/Downloads/wordcountwindow.txt");
        PrintStream stream = new PrintStream(file);
        System.setOut(stream);

        // get input data
        DataStream<String> text = env.readTextFile("/home/cpre419/Downloads/shakespeare");

        // this code is performed per input line
        DataStream<Tuple2<String,Integer>> counts =
              text
              .countWindowAll(1000)
              .apply(new AllWindowFunction<String, Tuple2<String,Integer>, GlobalWindow>() {  
                  public void apply(GlobalWindow window, Iterable<String> values, 
                          Collector<Tuple2<String,Integer>> out) throws Exception {
                      //Group by key and count frequency
                	  HashMap <String,Integer> countWord = new HashMap<String,Integer>();
                      
                      for (String value : values) {
                    	  
                    	  String[] tokens = value.toLowerCase().split("\\W+");
                    	  for (String token : tokens)
                    	  {	  if (token.length() >0) {
                    		  if (countWord.get(token) != null)
                              { countWord.put(token, countWord.get(token)+1);}
                              else
                              {countWord.put(token, 1);}
                    	  }
                    		  
                    	  }
                      }
                      
                      
                      //Comparator to sort in reverse
               			Comparator<String> comparator = new Comparator<String>() {
               			    public int compare(String o1, String o2) {
               			        return countWord.get(o1).compareTo(countWord.get(o2));
               			    }
               			};

               			ArrayList<String> words = new ArrayList<String>();
               			words.addAll(countWord.keySet());

               			Collections.sort(words,comparator);
               			Collections.reverse(words);
               			
//                           System.out.println(Arrays.asList(words));
               			System.out.println("10 most frequent words per stream");
                        System.out.println("-----------");
                       
                           for(int i = 0; i < 10; i++) {
                           	System.out.println(words.get(i) + " : " + countWord.get(words.get(i)));
                           }
                           
                           
                           System.out.println("\n");
                           
                  }
              
              
                  
              
              
              });

        // emit result
        
 
        env.execute("Streaming WordCount Aggregation Example");
        
    }
}