import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.sun.tools.javac.util.List;

import scala.Tuple2;
import scala.collection.Map;



public class NumOfTriangles {
	
	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		
		SparkConf sparkConf = new SparkConf().setAppName("Analyse Github data");
		// .setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> input = context.textFile(args[0]);
		
//		
		JavaRDD<String> graphData = input.flatMap(new FlatMapFunction<String, String>(){
			public Iterator<String> call(String s){
				return Arrays.asList(s.split("\n+")).iterator();
			}
		}).filter(f -> !f.isEmpty());
		
//		
//		graphData.saveAsTextFile(args[1]);
//		

		
		JavaPairRDD<Long, Long> edgeData = graphData.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
			public Iterator<Tuple2<Long, Long>> call(String s) {

				String[] t= s.split("\\s+");
				
				List<Tuple2<Long, Long>> res = new ArrayList<Tuple2<Long, Long>>();
				res.add(new Tuple2<Long, Long>(Long.parseLong(t[0]),Long.parseLong(t[1])));
				res.add(new Tuple2<Long, Long>(Long.parseLong(t[1]),Long.parseLong(t[0])));
				
				return res.iterator();
			}
		});
		
//		JavaPairRDD<Long,Iterable<Long>> adjList = edgeData.groupByKey();
		
//		List<Tuple2<Long, Iterable<Long>>> l = adjList.collect();
//		for(Tuple2<Long, Iterable<Long>> t : l) {
//			System.out.println(t._1 + " " + t._2);
//		}
		
		JavaPairRDD<Long,ArrayList<Long>> adjList = edgeData.groupByKey().mapToPair(f->{
			ArrayList<Long> l = new ArrayList<Long>();
			f._2().forEach(l::add);
			return new Tuple2<Long, ArrayList<Long>>(f._1(), l);	
		});
		

//		JavaPairRDD<Tuple2<Long,Long>, Long> pairs = adjList.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, ArrayList<Long>>, Tuple2<Long, Long>, Long>() {
//			public Iterator<Tuple2<Tuple2<Long, Long>, Long>> call(Tuple2<Long, ArrayList<Long>> node) {
//				
//				Long u = node._1();
//				ArrayList<Long> nbrs = node._2();
//				
//				//Use a set to handle duplicate <k,v> pairs
//				HashSet<Tuple2<Tuple2<Long,Long>, Long>> res = new HashSet<Tuple2<Tuple2<Long,Long>, Long>>();
//				
//				
//				//Emit <<u,v>,1>
//				for(Long v : nbrs) {
//					if(u != v) {
//						Tuple2<Long, Long> t = new Tuple2<Long, Long>(Math.min(u, v), Math.max(u, v));
//						res.add(new Tuple2<Tuple2<Long,Long>, Long>(t,(long) -1));
//					}
//				}
//				
//				//Emit <<x,y>, u> where u is the key and x,y are nodes taken pairwise
//				for(int i = 0; i < nbrs.size() - 1; i++) {
//					for(int j = i+1; j < nbrs.size(); j++) {
//						Long x = nbrs.get(i);
//						Long y = nbrs.get(j);
//						if(x != y) {
//							Tuple2<Long, Long> t = new Tuple2<Long, Long>(Math.min(x, y), Math.max(x, y));
//							res.add(new Tuple2<Tuple2<Long,Long>, Long>(t,u));
//						}
//					}
//				}
//				
//				//Put the contents into a list
//				List<Tuple2<Tuple2<Long,Long>, Long>> resList = new ArrayList<Tuple2<Tuple2<Long,Long>, Long>>();
//				resList.addAll(res);
//				
//				return resList.iterator();
//			}
//		});
//		
//		JavaPairRDD<Tuple2<Long, Long>,ArrayList<Long>> newAdjList = pairs.groupByKey().mapToPair(f->{
//			ArrayList<Long> l = new ArrayList<Long>();
//			f._2().forEach(l::add);
//			return new Tuple2<Tuple2<Long, Long>, ArrayList<Long>>(f._1(), l);	
//		});
//		

		JavaPairRDD<String, String> pairs = adjList.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, ArrayList<Long>>, String, String>() {
			public Iterator<Tuple2<String, String>> call(Tuple2<Long, ArrayList<Long>> node) {
				Long u = node._1();
				ArrayList<Long> nbrs = node._2();
				
				//Use a set to handle duplicate <k,v> pairs
				HashSet<Tuple2<String, String>> res = new HashSet<Tuple2<String, String>>();
				HashSet<String> used = new HashSet<String>();
				
				
				//Emit <<u,v>,-1> as <u_v,-1>
				for(Long v : nbrs) {
					if(u != v) {
						Tuple2<Long, Long> t = new Tuple2<Long, Long>(Math.min(u, v), Math.max(u, v));
						String k1 = t._1() + "_" + t._2();
						String v1 = "-1";
						res.add(new Tuple2<String, String>(k1,v1));
					}
				}
				
				//Emit <x_y, u> where u is the key and x,y are nodes taken pairwise
				for(int i = 0; i < nbrs.size() - 1; i++) {
					for(int j = i+1; j < nbrs.size(); j++) {
						Long x = nbrs.get(i);
						Long y = nbrs.get(j);
						if(x != y) {
							Tuple2<Long, Long> t = new Tuple2<Long, Long>(Math.min(x, y), Math.max(x, y));
							String k2 = t._1() + "_" + t._2();
							String v2 = Long.toString(u);
							res.add(new Tuple2<String, String>(k2,v2));
						}
					}
				}
				
				//Put the contents into a list
				List<Tuple2<String, String>> resList = new ArrayList<Tuple2<String, String>>();
				resList.addAll(res);
				
				return resList.iterator();
			}
		});
		
		JavaPairRDD<String,ArrayList<String>> newAdjList = pairs.groupByKey().mapToPair(f->{
			ArrayList<String> l = new ArrayList<String>();
			f._2().forEach(l::add);
			return new Tuple2<String, ArrayList<String>>(f._1(), l);	
		});
		
		
		
		JavaPairRDD<String, Long> triangles = newAdjList.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<String>>,String, Long>() {
			public Iterator<Tuple2<String, Long>> call(Tuple2<String, ArrayList<String>> node) {
				
				HashSet<String> nbrs = new HashSet<String>();
				nbrs.addAll(node._2());
				
				ArrayList<Tuple2<String, Long>> res = new ArrayList<Tuple2<String, Long>>();
				
				if(nbrs.contains("-1")){
					nbrs.remove("-1");
					for(String v : nbrs) {
						String[] edge = node._1().split("_");
						long v1  = Long.parseLong(edge[0]);
						long v2 = Long.parseLong(edge[1]);
						long v3 = Long.parseLong(v);
						
						ArrayList<Long> nodes = new ArrayList<Long>();
						nodes.add(v1);
						nodes.add(v2);
						nodes.add(v3);
						nodes.sort(null);
						
						String k = String.valueOf(nodes.get(0)) + "-" + String.valueOf(nodes.get(1)) + "-" + String.valueOf(nodes.get(2));
						res.add(new Tuple2<String, Long>(k,(long) 1));
					}
				}
				return res.iterator();
			}
		});

		
		JavaPairRDD<String, Long> uniqueTriangles = triangles.distinct();
//		uniqueTriangles.saveAsTextFile(args[1]);
		
		JavaPairRDD<Long,String> countTriangles = uniqueTriangles.mapToPair(new PairFunction<Tuple2<String,Long>,Long,String>(){
			public Tuple2<Long,String> call(Tuple2<String,Long> p){
				String triangle = p._1;
				long count = p._2;
				return new Tuple2<Long,String>(count,triangle);
			} 
		});
//		countTriangles.saveAsTextFile(args[1]);
//		
		Map<Long, Long> counts = countTriangles.countByKey();
		
		System.out.println(counts);
		
//		JavaRDD<Long> counts = countTriangles.countByValue(new Function2<Long, Long, Long>() {
//			public Long call(Long i1, Long i2) {
//				return i1 + i2;
//			}
//		}, numOfReducers);
		
		
//		JavaPairRDD<Long,String> finalCount = counts.mapToPair(new PairFunction<Tuple2 <String,Long>,Long,String>(){
//			public Tuple2<Long,String> call(Tuple2<String,Long> p){return new Tuple2<Long,String>(p._2,p._1);} });
		
		ArrayList<Long> l = new ArrayList<Long>();
		l.add(counts.get(1));
		
		JavaRDD<Long> finalCount = context.parallelize(l);
		finalCount.saveAsTextFile(args[1]);
		
		context.stop();
		context.close();
		
		
	}

}
