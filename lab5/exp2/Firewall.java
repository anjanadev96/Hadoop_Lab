
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Firewall {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Firewall in Spark");
		// .setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> ipTrace = context.textFile(args[0]);
		JavaRDD<String> rawBlock = context.textFile(args[1]);

		//Read ipTrace file is saved in JavaPairRDD the required format
		JavaPairRDD<Integer, String> ipTracePairs = ipTrace.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {

				String time = s.split("\\s+")[0];
				String connectionID = s.split("\\s+", 7)[1];
				String sourceIP = s.split("\\s+", 7)[2];
				String destIP = s.split("\\s+", 7)[4];
				

				String sourceIPtrimmed = sourceIP.split("\\.")[0] + "." + sourceIP.split("\\.")[1] + "."
						+ sourceIP.split("\\.")[2] + "." + sourceIP.split("\\.")[3];

				String destIPtrimmed = destIP.split("\\.")[0] + "." + destIP.split("\\.")[1] + "."
						+ destIP.split("\\.")[2] + "." + destIP.split("\\.")[3];

				return new Tuple2<Integer, String>(Integer.parseInt(connectionID),
						time + " " + sourceIPtrimmed + " " + destIPtrimmed);
			}
		});
		//Read rawblock file is saved to RDD variable in suitable format
		JavaPairRDD<Integer, String> rawBlockPairs = rawBlock.mapToPair(new PairFunction<String, Integer,String>() {
			public Tuple2<Integer, String> call(String s) {
				String connectionID = s.split("\\s+")[0];
				String actionTaken = s.split("\\s+")[1];
				return new Tuple2<Integer, String>(Integer.parseInt(connectionID), actionTaken);
			}
		});
		
		//Filter out Ips with status blocked and apply to rawblock data
		Function<Tuple2<Integer, String>, Boolean> blockFilter = t -> (t._2.equals("Blocked"));

		JavaPairRDD<Integer, String> filteredRb = rawBlockPairs.filter(blockFilter);


		JavaPairRDD<Integer, Tuple2<String, String>> joinedData = ipTracePairs.join(filteredRb).sortByKey();
		
		//Save data in the format required for generating firewall log file
		JavaRDD<String> firewallLog = joinedData.map(new Function<Tuple2<Integer, Tuple2<String, String>>, String>() {
			public String call(Tuple2<Integer, Tuple2<String, String>> p) {
				String time = p._2._1.split("\\s+")[0];
				String connectionID = String.valueOf(p._1);
				String sourceIP = p._2._1.split("\\s+")[1];
				String destIP = p._2._1.split("\\s+")[2];
				String blocked = p._2._2;

				return time + " " + connectionID + " " + sourceIP + " " + destIP + " " + blocked;
			}
		});

		// To each source IP, add one as a val, to pass to reducer to count
		JavaPairRDD<String, Integer> ones = firewallLog.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {

				String sourceIP = s.split("\\s+")[2];

				return new Tuple2<String, Integer>(sourceIP, 1);
			}
		});

		//Reduce operation to count all grouped source Ips
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);
		
		//Swap the count and key to pass to the sort function
		JavaPairRDD<Integer, String> swapOnes = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> p) {
						return p.swap();
					}
				});

		//Sort in descending order
		JavaPairRDD<Integer, String> sortedIP = swapOnes.sortByKey(false);

		firewallLog.saveAsTextFile(args[2]);
		sortedIP.saveAsTextFile(args[3]);
		context.stop();
		context.close();

	}

}
