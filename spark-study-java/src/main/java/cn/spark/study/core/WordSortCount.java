package cn.spark.study.core;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class WordSortCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("WordSortCount")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D:\\Spark\\txt\\spark.txt");
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		JavaPairRDD<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}});
		
		JavaPairRDD<String, Integer> wordCount = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}});
		
		JavaPairRDD<Integer, String> word2Count = wordCount.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		});
		
		JavaPairRDD<Integer, String> wordSortCount = word2Count.sortByKey(false);
		wordSortCount.foreach(new VoidFunction<Tuple2<Integer,String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._2 + " appears " + t._1 + " times.");
			}
		});
		sc.close();
	}
}
