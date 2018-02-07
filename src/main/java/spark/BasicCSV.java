package spark;

import java.io.StringReader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import au.com.bytecode.opencsv.CSVReader;
import scala.Tuple2;

public class BasicCSV {

	public static class ParseLine implements PairFunction<String, String, String[]> {
		private static final long serialVersionUID = 2836132784035323428L;

		@SuppressWarnings({ "resource", "rawtypes", "unchecked" })
		public Tuple2<String, String[]> call(String line) throws Exception {
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] elements = reader.readNext();
			String key = elements[0];
			return new Tuple2(key, elements);
		}
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		String inputFile = args[0];
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BasicCSV");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> csvFile1 = sc.textFile(inputFile);
		JavaPairRDD<String, String[]> csvData = csvFile1.mapToPair(new ParseLine());
		csvData.foreach(x->System.out.println(x));
		System.out.println(csvData.count());
	}

}
