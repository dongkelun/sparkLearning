package spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HelloWord {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(args.length);
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("pandas", "i like pandas"));
		System.out.println(lines);
		JavaRDD<String> textFile = sc.textFile("D:/Company/bigdata/spark-2.2.1-bin-hadoop2.7/README.md");
		System.out.println(textFile.count());
		sc.stop();

	}

}
