package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 数据格式：
 * 日期 用户 搜索词 城市 平台 版本
 *
 * 需求：
 * 1、筛选出符合查询条件（城市、平台、版本）的数据
 * 2、统计出每天搜索uv排名前3的搜索词
 * 3、按照每天的top3搜索词的uv搜索总次数，倒序排序
 */
public class DailyTop3Keyword {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("DailyTop3Keyword");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());
        JavaRDD<String> keywordRDD = sc.textFile("hdfs://spark1:9000/test/test.txt");
        // 筛选条件
        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("iphone"));
        queryParamMap.put("version", Arrays.asList("13.0", "13.1", "13.2"));
        // 广播变量，每个节点不用都拷贝副本，有利于提升计算效率
        Broadcast<Map<String, List<String>>> queryParamMapBC = sc.broadcast(queryParamMap);
        // 1、过滤不满足条件的数据
        JavaRDD<String>  filterKeywordRDD = keywordRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String log) throws Exception {
                Map<String, List<String>> queryParamMap = queryParamMapBC.value();
                String[] words = log.split(" ");
                if (! queryParamMap.get("city").contains(words[3])) {
                    return false;
                }
                if (! queryParamMap.get("platform").contains(words[4])) {
                    return false;
                }
                if (! queryParamMap.get("version").contains(words[5])) {
                    return false;
                }
                return true;
            }
        });
//        filterKeywordRDD.collect().forEach(p -> System.out.println(p));
        // 2、映射成(日期_搜索词, 用户)
        JavaPairRDD<String, String> dateWordUserRDD = filterKeywordRDD.mapToPair(
                new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] words = s.split(" ");
                return new Tuple2<>(words[0] + "_" + words[2], words[1]);
            }
        });
//        dateUserWordRDD.collect().forEach(p -> System.out.println(p));
        // 3、按照“日期_搜索词”分组
        JavaPairRDD<String, Iterable<String>> dateWordGroup = dateWordUserRDD.groupByKey();
//        dateWordGroup.collect().forEach(p -> System.out.println(p));
        // 4、每天每个搜索词的搜索用户，执行去重操作，获得其uv
        JavaPairRDD<String, Integer> dateWordUserUVRDD = dateWordGroup.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String dateWord = t._1;
                Iterator<String> users = t._2.iterator();
                List<String> distinctUsers = new ArrayList<>();
                while (users.hasNext()) {
                    String u = users.next();
                    if (! distinctUsers.contains(u)) {
                        distinctUsers.add(u);
                    }
                }

                return new Tuple2<>(dateWord, distinctUsers.size());
            }
        });
//        dateWordUserUVRDD.collect().forEach(p -> System.out.println(p));
        // 5、转化为DataFrame
        JavaRDD<Row> dateKeywordUvRowRDD = dateWordUserUVRDD.map(new Function<Tuple2<String,Integer>, Row>() {

            @Override
            public Row call(Tuple2<String, Integer> dateWordUserUV) throws Exception {
                String date = dateWordUserUV._1.split("_")[0];
                String word = dateWordUserUV._1.split("_")[1];
                int uv = dateWordUserUV._2;
                return RowFactory.create(date, word, uv);
            }
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("word", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame dateKeywordUvDF = sqlContext.createDataFrame(dateKeywordUvRowRDD, structType);
        dateKeywordUvDF.registerTempTable("daily_word_uv");
//        dateKeywordUvDF.show();
        // 6、使用Spark SQL开窗函数统计每日搜索uv前三的关键词，SQL语句必须使用HiveContext执行。

        DataFrame dateTop3KeyWordDF = sqlContext.sql("select date, word, uv from (" +
                "select date, word, uv, row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank " +
                "from daily_word_uv) tmp where rank <= 3");
        dateTop3KeyWordDF.show();
        // 7、按照每日top3搜索总uv降序排序
    }
}
