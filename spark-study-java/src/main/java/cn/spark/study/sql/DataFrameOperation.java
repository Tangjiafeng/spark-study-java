package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class DataFrameOperation {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("create").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.read().json("F:\\temp\\students.json");
		df.registerTempTable("students");
		DataFrame df2 = sqlContext.sql("SELECT * FROM students");
		df2.show();
		df.show();
		df.printSchema();
		df.select("name").show();
		df.select(df.col("name")).show();
		df.select(df.col("name"), df.col("age")).show();		
		df.filter(df.col("age").gt(20)).show();
		df.groupBy("age").count().show();
		sc.close();
	}
}
