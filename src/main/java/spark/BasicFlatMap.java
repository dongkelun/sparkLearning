package spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class BasicFlatMap {

	public static void main(String[] args) {
		SparkConf conf  = new SparkConf().setMaster("local").setAppName("Uinon RDD");
		@SuppressWarnings("resource")
		JavaSparkContext sc= new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			public Iterator<String> call(String line) {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		System.out.println(words.first());
		for(String line:words.take((int) words.count())){
			System.out.println(line);
		}
		
	}

}
