package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class UnionExample {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf conf  = new SparkConf().setMaster("local").setAppName("Uinon RDD");
		JavaSparkContext sc= new JavaSparkContext(conf);
		JavaRDD<String> inputRDD = sc.textFile("log.log");
		JavaRDD<String> errorRDD = inputRDD.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = -2952658994103736773L;

			@Override
			public Boolean call(String x) throws Exception {
				
				return x.contains("error");
			}
		});
		JavaRDD<String> warningsRDD = inputRDD.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = -2952658994103736773L;

			@Override
			public Boolean call(String x) throws Exception {
				
				return x.contains("error");
			}
		});
		JavaRDD<String> badLinesRDD = errorRDD.union(warningsRDD);
		System.out.println(badLinesRDD.count());
		for(String line:badLinesRDD.take(10)){
			System.out.println(line);
		}
	}

}
