package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Generic load and save operation.
 */
public class LoadData {
    public static void main( String[] args) {
        SparkConf conf = new SparkConf().setAppName("loadData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().load("E:\\spark\\parquet\\users.parquet");
        df.select("name").show();
        // save方法废弃
        df.select("name").saveAsParquetFile("E:\\spark\\parquet\\users_name.parquet");
    }
}
