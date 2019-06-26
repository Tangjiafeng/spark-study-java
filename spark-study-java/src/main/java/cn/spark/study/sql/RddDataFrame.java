package cn.spark.study.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RddDataFrame {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("RddDataFrame")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lines = sc.textFile("F:\\temp\\students.txt");
		JavaRDD<Student> studentRDD = lines.map(line -> {
			String[] parts = line.split(",");
			Student student = new Student();
			student.setName(parts[0]);
			student.setAge(Integer.parseInt(parts[1].trim()));
			return student;
		});
		
		DataFrame studentDF = sqlContext.createDataFrame(studentRDD, Student.class);
		studentDF.registerTempTable("student");
		DataFrame adultDF = sqlContext.sql("select * from student where age > 18");
		JavaRDD<Row> adultRDD = adultDF.javaRDD();
		
		List<Student> students = adultRDD.map(adult -> {
			Student std = new Student();
			std.setName(adult.getString(1));
			std.setAge(adult.getInt(0));
			return std;
		}).collect();
		
		for(Student s : students) {
			System.out.println(s.toString());
		}
		
		sc.close();
	}
}
