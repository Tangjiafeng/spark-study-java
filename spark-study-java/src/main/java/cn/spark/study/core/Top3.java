package cn.spark.study.core;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Top3 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Top3")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D:\\Spark\\txt\\top.txt");
		JavaPairRDD<Integer, String> numPairs = lines.mapToPair(new PairFunction<String, Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				return new Tuple2<Integer, String>(Integer.valueOf(t), t);
			}
		}).sortByKey(false);
		List<Tuple2<Integer, String>> numList = numPairs.take(3);
		for (int i = 0; i < numList.size(); i++) {
			System.out.println(numList.get(i)._1);
		}
		sc.close();
	}

}
