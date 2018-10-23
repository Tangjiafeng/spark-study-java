package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
public class SecondarySort {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SecondarySort")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D:\\Spark\\txt\\sort.txt");
		JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<SecondarySortKey, String> call(String t) throws Exception {
				List<String> strs = Arrays.asList(t.split(" "));
				SecondarySortKey ssk = new SecondarySortKey(Integer.parseInt(strs.get(0)), Integer.parseInt(strs.get(1)));
				return new Tuple2<SecondarySortKey, String>(ssk, t);
			}
		});
		
		JavaPairRDD<SecondarySortKey, String> sortedPair = pairs.sortByKey(false);
		sortedPair.foreach(new VoidFunction<Tuple2<SecondarySortKey,String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<SecondarySortKey, String> t) throws Exception {
				System.out.println(t._2);
			}
		});
		sc.close();
	}
}
