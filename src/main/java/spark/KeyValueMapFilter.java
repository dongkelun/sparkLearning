package spark;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @description
 * @author 		������
 * @time   		2018��1��26��
 *
 */
public class KeyValueMapFilter {

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	public static void main(String[] args) {
		String inputFile = args[0];
		// ����һ��Java�汾��Spark Context
		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// ��ȡ���ǵ���������
		JavaRDD<String> input = sc.textFile(inputFile);
		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String x) {
				return new Tuple2(x.split(" ")[0], x);
			}
		};
		Function<Tuple2<String, String>, Boolean> longWordFilter = new Function<Tuple2<String, String>, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> input) {
				return (input._2().length() < 20);
			}
		};
		JavaPairRDD<String, String> rdd = input.mapToPair(keyData);
		JavaPairRDD<String, String> result = rdd.filter(longWordFilter);
		Map<String, String> resultMap = result.collectAsMap();
		for (Entry<String, String> entry : resultMap.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}

}
