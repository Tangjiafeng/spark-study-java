package cn.spark.study.sql;

/**
 * Parquet数据具备自动分区，合并元数据的功能。
 * 自动分区：指的是在列存储，表分区的方式下，不同的列下的数据存入Parquet文件时，
 *          会自动根据不同分区文件夹的名字自动推断列属性；
 * 合并元数据：不同列的DataFrame存入同一个Parquet文件，自动合并所有列
 */
public class ParquetData {
    public static void main( String[] args) {    }
}
