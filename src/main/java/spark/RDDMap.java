package spark;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class RDDMap {

	public static void main(String[] args) {
		
		SparkConf conf  = new SparkConf().setMaster("local").setAppName("Uinon RDD");
		@SuppressWarnings("resource")
//		JavaSparkContext sc= new JavaSparkContext(conf);
		JavaSparkContext sc = new JavaSparkContext("spark://192.168.44.128:7077", "Uinon 11"
				);
		sc.addJar("target\\springMaven.jar");
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x) { return x*x; }
		});
		JavaRDD<Integer> sample = rdd.sample(false, 0.5,200);
			Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = 1L;

				public Integer call(Integer x, Integer y) { return x + y; }
			});
		System.out.println(sum);
		System.out.println(StringUtils.join(result.collect(), ","));
		System.out.println(StringUtils.join(sample.collect(), ","));

	}

}
