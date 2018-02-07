package spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @description
 * @author 		董可伦
 * @time   		2018年1月23日
 *
 */
public class WordCount {
	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		// 创建一个Java版本的Spark Context
		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 读取我们的输入数据
		JavaRDD<String> input = sc.textFile(inputFile);
		// 切分为单词
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		// 转换为键值对并计数
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		//测试内置函数
		Map<String, Long> countByValue = words.countByValue();
		System.out.println(countByValue);
		// 将统计出来的单词总数存入一个文本文件，引发求值
		counts.saveAsTextFile(outputFile);
	}
}
