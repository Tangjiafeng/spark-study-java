package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

public class JDBCData {
    public static void main( String[] args) {
        SparkConf sc = new SparkConf().setAppName("JDBC_data").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        // 连接数据库
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/hibernate?user=root&password=5211314");
        options.put("dbtable", "s2");
        DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();
        jdbcDF.show();
    }
}
