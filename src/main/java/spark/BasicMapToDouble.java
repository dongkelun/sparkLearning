package spark;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;

public class BasicMapToDouble {

	public static void main(String[] args) {
		String master;
		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local";
		}
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(master, "basicmaptodouble", System.getenv("SPARK_HOME"),
				System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		rdd.filter(new Function<Integer,Boolean>(){

			private static final long serialVersionUID = 1L;

			public Boolean call(Integer v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
			
		});
		JavaDoubleRDD doubleResult = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0));
		System.out.println(doubleResult.mean());
		JavaDoubleRDD result = rdd.mapToDouble(new DoubleFunction<Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public double call(Integer x) {
				double y = (double) x;
				return y * y;
			}
		});
		System.out.println(StringUtils.join(result.collect(), ","));
		System.out.println(result.mean());
	}

}
