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
 * @author 		������
 * @time   		2018��1��23��
 *
 */
public class WordCount {
	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		// ����һ��Java�汾��Spark Context
		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// ��ȡ���ǵ���������
		JavaRDD<String> input = sc.textFile(inputFile);
		// �з�Ϊ����
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		// ת��Ϊ��ֵ�Բ�����
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
		//�������ú���
		Map<String, Long> countByValue = words.countByValue();
		System.out.println(countByValue);
		// ��ͳ�Ƴ����ĵ�����������һ���ı��ļ���������ֵ
		counts.saveAsTextFile(outputFile);
	}
}
