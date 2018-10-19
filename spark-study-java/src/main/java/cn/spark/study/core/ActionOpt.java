package cn.spark.study.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class ActionOpt {

	public static void main(String[] args) {
//		reduce();
//		collect();
//		count();
//		take();
//		saveAsTestFile();
		countByKey();
	}
	
	public static void reduce() {
		SparkConf conf = new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 模拟数据
		List<Integer> nums = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numRDD = sc.parallelize(nums);
		int sum = numRDD.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println("总和：" + sum);
		sc.close();
	}
	
	public static void collect() {
		SparkConf conf = new SparkConf()
				.setAppName("collect")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		JavaRDD<Integer> doubleNumbers = numbers.cache().map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
		}});
		// collect() 方法会把数据拉取到本地，发生大量网络传输，故不建议使用；替代方案是 RDD 的 foreach 操作
		List<Integer> doubleNumberList = doubleNumbers.collect();
		System.out.println(Arrays.toString(doubleNumberList.toArray()));
		sc.close();
	}
	
	public static void count() {
		SparkConf conf = new SparkConf()
				.setAppName("count")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		long len = numbers.count();
		System.out.println("总数：" + len);
		sc.close();
	}
	
	public static void take() {
		SparkConf conf = new SparkConf()
				.setAppName("take")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		JavaRDD<Integer> doubleNumbers = numbers.cache().map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
		}});
		// take() 方法与 collect() 方法类似
		List<Integer> doubleNumberList = doubleNumbers.take(5);
		System.out.println(Arrays.toString(doubleNumberList.toArray()));
		sc.close();
	}
	
	public static void saveAsTestFile() {
		// 创建SparkConf和JavaSparkContext
		SparkConf conf = new SparkConf()
				.setAppName("saveAsTextFile");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 有一个集合，里面有1到10,10个数字，现在要对10个数字进行累加
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);

		// 直接将rdd中的数据，保存在HFDS文件中
		// 但是要注意，我们这里只能指定文件夹，也就是目录
		// 那么实际上，会保存为目录中的/double_number.txt/part-00000文件
		numbers.saveAsTextFile("hdfs://spark1:9000/number");

		// 关闭JavaSparkContext
		sc.close();
	}
	
	public static void countByKey() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("countByKey")
				.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 模拟集合
		List<Tuple2<String, String>> scoreList = Arrays.asList(
				new Tuple2<String, String>("class1", "leo"),
				new Tuple2<String, String>("class2", "jack"),
				new Tuple2<String, String>("class1", "marry"),
				new Tuple2<String, String>("class2", "tom"),
				new Tuple2<String, String>("class2", "david"));

		// 并行化集合，创建JavaPairRDD
		JavaPairRDD<String, String> students = sc.parallelizePairs(scoreList);
		
		Map<String, Object> studentCount = students.countByKey();
		
		for (Map.Entry<String, Object> entry : studentCount.entrySet()) {
			System.out.println("class = " + entry.getKey() + ", count = " + entry.getValue());
		}
		sc.close();
	}
	
	public static void foreach() {
		
	}
}
