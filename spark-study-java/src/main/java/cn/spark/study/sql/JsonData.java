package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 读取JSON数据源，并进行join操作
 * 获取成绩大于80分学生信息
 * t_score:name, score
 * t_infos:name, age
 * 合并之后：name, score, age
 */
public class JsonData {
    public static void main( String[] args) {
        SparkConf sc = new SparkConf().setAppName("json_data").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        DataFrame studentScoreDF = sqlContext.read().json("E:\\spark\\json\\students.json");
//        studentScoreDF.show();
        studentScoreDF.registerTempTable("t_score");
        DataFrame goodStudentScoreDF = sqlContext.sql("select * from t_score where score > 80");
//        goodStudentScoreDF.show();
        List<String> goodStudentNames = goodStudentScoreDF.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();
//        goodStudentNames.forEach(name -> System.out.println(name));
        List<String> infosJSON = new ArrayList<>();
        infosJSON.add("{\"name\":\"Leo\", \"age\":18}");
        infosJSON.add("{\"name\":\"Marry\", \"age\":20}");
        infosJSON.add("{\"name\":\"Jack\", \"age\":19}");
        JavaRDD<String> infosRDD = javaSparkContext.parallelize(infosJSON);
        DataFrame studentInfoDF = sqlContext.read().json(infosRDD);
//        studentInfoDF.show();
        studentInfoDF.registerTempTable("t_infos");
        StringBuffer sql = new StringBuffer("select name, age from t_infos where name in (");
        for (String s : goodStudentNames) {
            sql.append("'" + s + "',");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
//        System.out.println(sql.toString());
        DataFrame goodStudentInfosDF = sqlContext.sql(sql.toString());
//        goodStudentInfosDF.show();
        // join操作按照键值匹配结果整合数据：<k,v>.join(<k,w>) -> <k,v,w>
        JavaPairRDD<String, Tuple2<Integer, Integer>> resultRDD = goodStudentScoreDF.toJavaRDD()
                .mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join(goodStudentInfosDF.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }));
        // RDD转List可以直接打印数据
        resultRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return t._1 + " " + t._2._1 + " " + t._2._2;
            }
        }).collect().forEach(s -> System.out.println(s));
        // 通过编程方式将RDD转化成DataFrame，进行sql操作
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = resultRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1.toString(), t._2._1, t._2._2);
            }
        });
        DataFrame resultDF = sqlContext.createDataFrame(rowJavaRDD, schema);
        resultDF.show();
    }
}
