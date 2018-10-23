package cn.spark.study.core;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class ShareVariable {

	public static void main(String[] args) {
		// broadCast();
		accumulate();
	}
	
	public static void broadCast() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		/*
		 * Spark 提供的 Broadcast Variable，是只读的。并且在每个节点上只会有一份副本，而不会为每个 task 都拷贝一份副本。
		 * 因此其最大作用，就是减少变量到各个节点的网络传输消耗，以及在各个节点上的内存消耗。
		 * 此外， spark 自己内部也使用了高效的广播算法来减少网络消耗
		 */
		final int factor = 5;
		final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
		// 并行化处理集合
		JavaRDD<Integer> numbersRDD = sc.parallelize(Arrays.asList(1,2,3,4,5));
		// map 算子，接收 Function 函数类型，指定泛型参数（前者为传入参数类型，后者为返回类型）
		JavaRDD<Integer> multipleRDD = numbersRDD.map( new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				int v2 = factorBroadcast.value(); 
				return v * v2; 
			}
			
		});
		
		multipleRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer v) throws Exception {
				System.out.println(v);
			}
			
		});
		sc.close();
	}
	
	public static void accumulate() {
		SparkConf conf = new SparkConf().setAppName("TransformationOpt").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		/*
		 * Spark 提供的 Accumulator，主要用于多个节点对一个变量进行共享性的操作。Accumulator 只提供了累加的功能。
		 * 但是确给我们提供了多个 task 对一个变量并行操作的功能。但是 task 只能对 Accumulator 进行累加操作，不能读取它的值。
		 * 只有 Driver 程序可以读取 Accumulator 的值。
		 */
		final Accumulator<Integer> sum = sc.accumulator(0);
		// 并行化处理集合
		JavaRDD<Integer> numbersRDD = sc.parallelize(Arrays.asList(1,2,3,4,5));
		
		numbersRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Integer v) throws Exception {
				sum.add(v);
			}
		});
		System.out.println(sum.value());
		sc.close();
	}
}
