
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;



import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;
import java.io.PrintStream;
import java.util.*;

@SuppressWarnings("serial")
public class BigramsWindow {
        
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        
        //set output file
        File file = new File("/home/cpre419/Downloads/bigramwindow.txt");
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
                      
                	  HashMap <String,Integer> countBigram = new HashMap<String,Integer>();
                      //For each line
                      for (String value : values) {
                    	  
                    	String line = value.toString();
//                        System.out.println(line);
                        line = line.toLowerCase();
//                        System.out.println(line);
                        line = line.replaceAll("[^a-z0-9.?! ]","");
//                        System.out.println(line);
                        line = line.replaceAll("[.]", " . ");
//                        System.out.println(line);
                        line = line.replaceAll("[?]", " . ");
//                        System.out.println(line);
                        line = line.replaceAll("[!]", " . ");
                        
                        StringTokenizer tokenizer = new StringTokenizer(line);

                        String prev = ".";
                        //While there are words in the tokenizer output
                        while(tokenizer.hasMoreTokens()){

                            String current = tokenizer.nextToken();

                            //If previous is an end of sentence indicator, ignore
                            if(!prev.contains(".") && !current.contains(".")) {
                            	String token =  prev + " " + current;
//                            	System.out.println(token);
                            	if (token.length() >0) {
                          		  if (countBigram.get(token) != null)
                                    { countBigram.put(token, countBigram.get(token)+1);}
                                    else
                                    {countBigram.put(token, 1);}
                          	  }
                            	
                            }
                                
                            prev = current;
                        }
                    	  	  
                      }
                      
                      Comparator<String> comparator = new Comparator<String>() {
             			    public int compare(String o1, String o2) {
             			        return countBigram.get(o1).compareTo(countBigram.get(o2));
             			    }
             			};
             			
             			ArrayList<String> words = new ArrayList<String>();
               			words.addAll(countBigram.keySet());

               			Collections.sort(words,comparator);
               			Collections.reverse(words);
               			
//                           System.out.println(Arrays.asList(words));
               			   System.out.println("10 most frequent bigrams per stream");
                           System.out.println("-----------");
                       
                           for(int i = 0; i < 10; i++) {
                           	System.out.println(words.get(i) + " : " + countBigram.get(words.get(i)));
                           }
                           
                      
                          System.out.println("\n");
               			
              
              
                  }
              
              
              });

        // emit result
        
 
        env.execute("Streaming Bigrams Aggregation Example");
        
    }
}