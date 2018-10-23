import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * RDD 持久化
 * @author A
 *
 */
public class Persist {

	public static void main(String[] args) {
		// 创建SparkConf和JavaSparkContext
		SparkConf conf = new SparkConf()
				.setAppName("saveAsTextFile").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 大文件
		JavaRDD<String> lines = sc.textFile("D:\\Spark\\txt\\big.txt").persist(StorageLevel.MEMORY_ONLY());
		long beginTime = System.currentTimeMillis();
		long count = lines.count();
		long endTime = System.currentTimeMillis();
		System.out.println("cost time:" + (endTime - beginTime));
		System.out.println("line's count : " + count);
		long begin2Time = System.currentTimeMillis();
		long count2 = lines.count();
		long end2Time = System.currentTimeMillis();
		System.out.println("cost time:" + (end2Time - begin2Time));
		System.out.println("line's count : " + count2);
		sc.close();
	}
}
