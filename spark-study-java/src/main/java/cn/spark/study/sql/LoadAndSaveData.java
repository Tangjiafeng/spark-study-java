package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.SaveMode.*;

/**
 * Generic load and save operation.
 */
public class LoadData {
    public static void main( String[] args) {
        SparkConf conf = new SparkConf().setAppName("loadData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // 默认load的文件格式为.parquet，.parquet文件在windows平台有权限问题，load方法报错
//        DataFrame df1 = sqlContext.read().load("E:\\spark\\parquet\\users.parquet");
        // 亦可指定.parquet文件格式
//        DataFrame df2 = sqlContext.read().parquet("E:\\spark\\parquet\\users.parquet");

        // 可手动指定数据源的格式
        DataFrame df3 = sqlContext.read().format("json").load("E:\\spark\\json\\people.json");
        // this.json(String path)的内部调用是this.format("json").load(path);
        DataFrame df4 = sqlContext.read().json("E:\\spark\\json\\people.json");
//        df1.select("name").show();
//        df2.select("name").show();
        df3.select("age").show();
        df4.select("name").show();
        // 指定保存路径，windows平台会报错
//        df4.select("name").write().save("E:\\spark\\json\\people_name.json");
        // 并制定保存数据格式
//        df4.select("name").write().format("json").save("E:\\spark\\json\\people_name.json");
    }
}
