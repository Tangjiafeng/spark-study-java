package cn.spark.study.core;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupTop3 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Top3")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D:\\Spark\\txt\\score.txt");
		JavaPairRDD<String, Integer> classScore = lines.mapToPair(
				new PairFunction<String, String, Integer>(){
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						String[] strs = t.split(" ");
						return new Tuple2<String, Integer>(strs[0], Integer.valueOf(strs[1]));
					}
				});
		JavaPairRDD<String, Iterable<Integer>> groupedScore = classScore.groupByKey();
		groupedScore.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println(t._1 + ":");
				Integer[] top3 = new Integer[3];
				
				Iterator<Integer> scores = t._2.iterator();
				
				while(scores.hasNext()) {
					Integer score = scores.next();
					
					for(int i = 0; i < 3; i++) {
						if(top3[i] == null) {
							top3[i] = score;
							break;
						} else if(score > top3[i]) {
							for(int j = 2; j > i; j--) {
								top3[j] = top3[j - 1];  
							}
							
							top3[i] = score;
							
							break;
						} 
					}
				}
				for(int i = 0; i < 3; i++) {
					System.out.println(top3[i]);
				}
			}
		});
		
		sc.close();
	}

}
