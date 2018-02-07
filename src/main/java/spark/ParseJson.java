package spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

public class ParseJson implements FlatMapFunction<Iterator<String>, HashMap> {

	private static final long serialVersionUID = -6120880849464887070L;

	@SuppressWarnings("rawtypes")
	public Iterator<HashMap> call(Iterator<String> lines) throws Exception {
		ArrayList<HashMap> people = new ArrayList<HashMap>();
		ObjectMapper mapper = new ObjectMapper();
		while (lines.hasNext()) {
			String line = lines.next();
			try {
				people.add(mapper.readValue(line, HashMap.class));
			} catch (Exception e) {
				// 跳过失败的数据
				System.out.println("ee");
			}
		}
		return people.iterator();
	}
	@SuppressWarnings("rawtypes")
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
		JavaRDD<String> input = sc.textFile("file/file.json");
		JavaRDD<HashMap> result = input.mapPartitions(new ParseJson());
		result.foreach(x -> System.out.println(x));
		result.count();
		for(HashMap s:result.take((int) result.count())){
			System.out.println(s);
		}
//		JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
//		formatted.saveAsTextFile("file/outfile");
	}
}
