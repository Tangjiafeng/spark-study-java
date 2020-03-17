package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Parquet数据具备自动分区，合并元数据的功能。
 * 自动分区：指的是在列存储，表分区的方式下，不同的列下的数据存入Parquet文件时，
 *          会自动根据不同分区文件夹的名字自动推断列属性；
 * 合并元数据：不同列的DataFrame存入同一个Parquet文件，自动合并所有列
 */
public class LoadParquetData {
    public static void main( String[] args) {
        SparkConf conf = new SparkConf().setAppName("loadParquetData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame userDF = sqlContext.read().parquet("E:\\spark\\parquet\\users.parquet");
        userDF.registerTempTable("t_user");
        DataFrame nameDF = sqlContext.sql("select name from t_user");
        List<String> names = nameDF.javaRDD().map(new Function<Row, String>() {

            @Override
            public String call(Row v1) throws Exception {
                return "name" + v1.getString(0);
            }
        }).collect();
        for (String name : names) {
            System.out.println(name);
        }
    }
}
