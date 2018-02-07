package spark;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class BasicAvg {

	public static class AvgCount implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public AvgCount(int total, int num) {
			total_ = total;
			num_ = num;
		}

		public int total_;
		public int num_;

		public float avg() {
			return total_ / (float) num_;
		}
	}

	public static void main(String[] args) {
		String master;
		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local";
		}
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"),
				System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public AvgCount call(AvgCount a, Integer x) {
				System.out.println(a.total_ + " " + a.num_);
				a.total_ += x;
				a.num_ += 1;
				return a;
			}
		};
		Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public AvgCount call(AvgCount a, AvgCount b) {
				System.out.println(a.total_ + " " + b.total_);
				System.out.println(a.num_ + " " + b.num_);
				a.total_ += b.total_;
				a.num_ += b.num_;
				return a;
			}
		};
		AvgCount initial = new AvgCount(0, 0);
		AvgCount result = rdd.aggregate(initial, addAndCount, combine);
		System.out.println(result.avg());
		sc.stop();

	}

}
