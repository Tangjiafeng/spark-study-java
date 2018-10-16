package cn.spark.study.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * transformation 操作实战
 * @author A
 *
 */
public class TransformationOpt {

	public static void main(String[] args) {
//		map();
//		filter();
//		flatMap();
//		group();
//		reduce();
		join();
//		coGroup();
	}
	/*
	 * map 操作，将集合中的数字乘以 2
	 */
	public static void map() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 并行化处理集合
		JavaRDD<Integer> numbersRDD = sc.parallelize(Arrays.asList(1,2,3,4,5));
		// map 算子，接收 Function 函数类型，指定泛型参数（前者为传入参数类型，后者为返回类型）
		JavaRDD<Integer> doubleRDD = numbersRDD.map( new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
			}
			
		});
		
		doubleRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer v) throws Exception {
				System.out.println(v);
			}
			
		});
		sc.close();
	}
	/*
	 * filter 操作，按照条件过滤集合，提取数字中的偶数
	 */
	public static void filter() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 并行化处理集合
		JavaRDD<Integer> numbersRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
		// filter 算子，接收 Function 函数类型，指定泛型参数（前者为传入参数类型，后者为返回类型），条件为真的元素被保留下来
		JavaRDD<Integer> newNumberRDD = numbersRDD.filter(new Function<Integer, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer v) throws Exception {
				return v % 2 == 0;
			}
		});
		newNumberRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
			
		});
		sc.close();
	}
	/*
	 * flatMap 操作，将多行语句分解成单词的集合
	 */
	public static void flatMap() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> strs = new ArrayList<String>();
		strs.add("I am a doctor");
		strs.add("Can you save me?");
		strs.add("Yes I can");
		strs.add("Thankyou just give me the medicine");
		strs.add("OK, take it darling");
		// 并行化处理集合
		JavaRDD<String> lines = sc.parallelize(strs);
		// flatMap 算子，接收 FlatMapFunction 函数类型，指定泛型参数（前者为传入参数类型，后者为返回类型）
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		words.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		sc.close();
	}
	/*
	 * group 操作，按照键值将数据分组
	 */
	public static void group() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 模拟集合
		List<Tuple2<String, Integer>> records = Arrays.asList(
				new Tuple2<String, Integer>("c1", 80),
				new Tuple2<String, Integer>("c3", 75),
				new Tuple2<String, Integer>("c1", 90),
				new Tuple2<String, Integer>("c2", 85),
				new Tuple2<String, Integer>("c1", 75),
				new Tuple2<String, Integer>("c2", 66));
		// 并行化处理集合
		JavaPairRDD<String, Integer> recordsRDD = sc.parallelizePairs(records);
		// 注意第二个泛型为 Iterable 类型
		JavaPairRDD<String, Iterable<Integer>> recordPairRDD = recordsRDD.groupByKey();
		recordPairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println(t._1);
				Iterator<Integer> it = t._2.iterator();
				while(it.hasNext()) {
					System.out.print(it.next() + " ");
				}
				System.out.println("\n====================");
			}
		});
		sc.close();
	}
	/*
	 * reduce 操作
	 */
	public static void reduce() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 模拟集合
		List<Tuple2<String, Integer>> records = Arrays.asList(
				new Tuple2<String, Integer>("c1", 80),
				new Tuple2<String, Integer>("c3", 75),
				new Tuple2<String, Integer>("c1", 90),
				new Tuple2<String, Integer>("c2", 85),
				new Tuple2<String, Integer>("c1", 75),
				new Tuple2<String, Integer>("c2", 66));
		// 并行化处理集合
		JavaPairRDD<String, Integer> recordsRDD = sc.parallelizePairs(records);
		// 注意第二个泛型为 Iterable 类型
		JavaPairRDD<String, Integer> recordPairRDD = recordsRDD.reduceByKey(
				new Function2<Integer,Integer,Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		JavaPairRDD<String, Integer> SortedRecordPairRDD = recordPairRDD.sortByKey();
		recordPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + " total score " + t._2);
			}
		});
		SortedRecordPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + " total score " + t._2);
			}
		});
		sc.close();
	}
	
	/*
	 * join 操作
	 */
	public static void join() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 模拟集合
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(3, "tom"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(3, 60));
		
		List<Tuple2<Integer, String>> otherList = Arrays.asList(
				new Tuple2<Integer, String>(1, "oth1"),
				new Tuple2<Integer, String>(2, "oth2"),
				new Tuple2<Integer, String>(3, "oth3"));
		
		// 并行化处理集合
		JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentScore = students.join(scores);
		
		studentScore.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
					throws Exception {
				System.out.println("student id: " + t._1);
				System.out.println("student name: " + t._2._1);
				System.out.println("student score: " + t._2._2);
				System.out.println("===============================");   
			}
		});
		// 注意 join 三个以上 JavaPairRDD 泛型的变化，新的元组的嵌套规则，以及取值下标
		JavaPairRDD<Integer, Tuple2<Tuple2<String, Integer>, String>> studentScoreOther = studentScore.join(sc.parallelizePairs(otherList));
		studentScoreOther.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Tuple2<String,Integer>, String>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Integer, Tuple2<Tuple2<String, Integer>, String>> t) throws Exception {
				System.out.println("student id: " + t._1);
				System.out.println("student name: " + t._2._1._1);
				System.out.println("student score: " + t._2._1._2);
				System.out.println("student other: " + t._2._2);
				System.out.println("===============================");
			}
		});
		sc.close();
	}
	
	public static void coGroup() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 模拟集合
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(3, "tom"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(1, 80),
				new Tuple2<Integer, Integer>(2, 70),
				new Tuple2<Integer, Integer>(3, 60));
		// 并行化处理集合
		JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
		// 与 join() 不同的是，对应于同一 key 值的多个 value 值会存放在一个集合之中
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores = 
				students.cogroup(scores);
		
		studentScores.foreach(
				
		new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
					throws Exception {
				System.out.println("student id: " + t._1);  
				System.out.println("student name: " + t._2._1);  
				System.out.println("student score: " + t._2._2);
				System.out.println("===============================");   
			}
			
		});
		
		sc.close();
	}
}
