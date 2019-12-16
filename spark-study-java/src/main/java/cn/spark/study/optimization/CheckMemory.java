package cn.spark.study.optimization;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.clearspring.analytics.stream.Counter;

public class CheckMemory {
	public static void main(String[] args) {
//		Counter counter = new Counter();
		SparkConf conf = new SparkConf().setAppName("CheckMemory").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> line = sc.textFile("D:\\Spark\\txt\\spark.txt").cache();
		sc.close();
	}
}
