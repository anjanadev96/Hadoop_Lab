

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class NumOfRepo {
	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(1);
		}
		

		SparkConf sparkConf = new SparkConf().setAppName("Analyse Github data");
		// .setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> githubData = context.textFile(args[0]);
		
		//Generate Pairs as language and rest of the data
		JavaPairRDD<String, String> githubPairs = githubData.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {

				String repo = s.split(",")[0];
				String lang = s.split(",")[1];
				String rating = s.split(",")[12];
				
//				
				return new Tuple2<String, String>(lang,
						"1" + " " + repo + " " + rating);
			}
		});
		
		//Reduce by language and return count,most ppular repo and number of stars
		JavaPairRDD<String, String> counts = githubPairs.reduceByKey(new Function2<String, String, String>() {
			public String call(String s, String t) {
				
				String[] splits1 = s.split("\\s");
				String[] splits2 = t.split("\\s");
				String count = Integer.toString(Integer.parseInt(splits1[0]) + Integer.parseInt(splits2[0]));
				int x = Integer.parseInt(splits1[2]);
				int y = Integer.parseInt(splits2[2]);
				
				if(x > y) {return count+" "+splits1[1]+" "+splits1[2];}
				else
				{return count+" "+splits2[1]+" "+splits2[2];}
				
				
			}
		}, numOfReducers);
		
		//Rearrange order, first count, then rest of the data
		JavaPairRDD<Integer,String> swapCounts = counts.mapToPair(new PairFunction<Tuple2 <String,String>,Integer,String>(){
			public Tuple2<Integer,String> call(Tuple2<String,String> p){
				
				String key = p._1;
				String s[] = p._2.split("\\s");
				int NumRepo = Integer.parseInt(s[0]);
				String rest = key +  " "+ s[1]+ " "+ s[2];
				
				
				
				
				
				return new Tuple2<Integer,String>(NumRepo,rest);} });
		
		//Sort Language popularity in descending
		JavaPairRDD<Integer,String> sorted = swapCounts.sortByKey(false);
		
		
		//Save in desired format
		JavaRDD<String> repoData = sorted.map(new Function <Tuple2<Integer, String>, String>() {
			public String call(Tuple2 <Integer, String> p) {
				String numRepo = Integer.toString(p._1);
				String s[] = p._2.split("\\s");
				String lang = s[0];
				String repoHighest = s[1];
				String numRatings = s[2];
				return lang + " " + numRepo + " " + repoHighest + " " + numRatings;
			}
		});
		
		repoData.saveAsTextFile(args[1]);
		
	
	
		context.stop();
		context.close();
		
	
	
	
	
	}

}
