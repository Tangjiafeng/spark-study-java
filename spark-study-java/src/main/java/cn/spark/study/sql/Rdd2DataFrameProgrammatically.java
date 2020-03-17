package cn.spark.study.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Rdd2DataFrameProgrammatically {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("RddDataFrame")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lines = sc.textFile("E:\\spark\\txt\\students.txt");
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		/*
		String schemaString = "name age";
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		*/
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = lines.map( line -> {
			String[] attributes = line.split(",");
			return RowFactory.create(attributes[1].trim(), Integer.parseInt(attributes[2]));// 注意字段顺序
		});
		DataFrame schemaDF = sqlContext.createDataFrame(rowRDD, schema);
		
		schemaDF.show();
		sc.close();
	}
}
