package spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class ChapterSixExample {
	// function2 前两个为参数，后一个为返回类型
	public static class SumInts implements Function2<Integer, Integer, Integer> {
		public Integer call(Integer x, Integer y) {
			return x + y;
		}
	}
//测试冲突
	public static class VerifyCallLogs implements Function<CallLog[], CallLog[]> {
		public CallLog[] call(CallLog[] input) {
			ArrayList<CallLog> res = new ArrayList<CallLog>();
			if (input != null) {
				for (CallLog call : input) {
					if (call != null && call.mylat != null && call.mylong != null && call.contactlat != null
							&& call.contactlong != null) {
						res.add(call);
					}
				}
			}
			return res.toArray(new CallLog[0]);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws FileNotFoundException {

		String inputFile = args[0];
		String outputDir = args[1];
		SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rdd = sc.textFile(inputFile);
		final Accumulator<Integer> blankLines = sc.accumulator(0);

		JavaRDD<String> callSigns = rdd.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 3953576891201438506L;

			public Iterator<String> call(String line) {
				if (line.trim().equals("")) {
					blankLines.add(1);
				} else {

				}
				return Arrays.asList(line.split("\\s+")).iterator();
			}
		}).filter(new Function<String,Boolean>(){

			@Override
			public Boolean call(String line) throws Exception {
				return line.length()>0;
			}
			
		}).persist(StorageLevel.DISK_ONLY());

		System.out.println(callSigns.count());
				callSigns.saveAsTextFile(outputDir + "/callsigns");
				
		System.out.println("Blank lines: " + blankLines.value());

		final Accumulator<Integer> validSignCount = sc.accumulator(0);
		final Accumulator<Integer> invalidSignCount = sc.accumulator(0);
		JavaRDD<String> validCallSigns = callSigns.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = -1940586553075693970L;

			public Boolean call(String callSign) {
				Pattern p = Pattern.compile("\\A\\d?\\p{Alpha}{1,2}\\d{1,4}\\p{Alpha}{1,3}\\Z");
				Matcher m = p.matcher(callSign);
				boolean b = m.matches();
				if (b) {
					validSignCount.add(1);
				} else {
					invalidSignCount.add(1);
				}
				return b;
			}
		});
		JavaPairRDD<String, Integer> contactCounts = validCallSigns
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 4894515692398235639L;

					@SuppressWarnings({ "unchecked", "rawtypes" })
					public Tuple2<String, Integer> call(String callSign) {
						return new Tuple2(callSign, 3);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 8991970874021609371L;

					//如果没有相同的key就不会走到reduceByKey的方法体了
					public Integer call(Integer x, Integer y) {
						System.out.println(1111111111);
						return x + y;
					}
				});
		// Force evaluation so the counters are populated
		contactCounts.count();
		System.out.println(invalidSignCount.value());
		System.out.println(validSignCount.value());
		if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
			//contactCounts.saveAsTextFile(outputDir + "/contactCount");
		} else {
			System.out.println("Too many errors " + invalidSignCount.value() + " for " + validSignCount.value());
			System.exit(1);
		}

		// Read in the call sign table
		// Lookup the countries for each call sign in the
		// contactCounts RDD.

		// 广播类型只读
		final Broadcast<String[]> signPrefixes = sc.broadcast(loadCallSignTable());
		JavaPairRDD<String, Integer> countryContactCounts = contactCounts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
					public Tuple2<String, Integer> call(Tuple2<String, Integer> callSignCount) {
						String sign = callSignCount._1();
						System.out.println(sign);
						String country = lookupCountry(sign, signPrefixes.value());
						System.out.println(callSignCount._2());
						return new Tuple2(country, callSignCount._2());
					}
				}).reduceByKey(new SumInts());
		countryContactCounts.count();
		//countryContactCounts.saveAsTextFile(outputDir + "/countries.txt");
		System.out.println("Saved country contact counts as a file");

		// Use mapPartitions to re-use setup work.
		JavaPairRDD<String, CallLog[]> contactsContactLists = validCallSigns
				//第一个为参数，后面两个返回类型
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, CallLog[]>() {
					public Iterator<Tuple2<String, CallLog[]>> call(Iterator<String> input) {
						// List for our results.
						ArrayList<Tuple2<String, CallLog[]>> callsignQsos = new ArrayList<Tuple2<String, CallLog[]>>();
						ArrayList<Tuple2<String, ContentExchange>> requests = new ArrayList<Tuple2<String, ContentExchange>>();
						ObjectMapper mapper = createMapper();
						HttpClient client = new HttpClient();
						try {
							client.start();
							while (input.hasNext()) {
								requests.add(createRequestForSign(input.next(), client));
							}
							for (Tuple2<String, ContentExchange> signExchange : requests) {
								callsignQsos.add(fetchResultFromRequest(mapper, signExchange));
							}
						} catch (Exception e) {
						}
						return callsignQsos.iterator();
					}
				});
		System.out.println(StringUtils.join(contactsContactLists.collect(), ","));
		String distScript = System.getProperty("user.dir") + "/src/R/finddistance.R";
		System.out.println(distScript);
		String distScriptName = "finddistance.R";
		sc.addFile(distScript);
		JavaRDD<String> pipeInputs = contactsContactLists.values().map(new VerifyCallLogs())
				.flatMap(new FlatMapFunction<CallLog[], String>() {
					public Iterator<String> call(CallLog[] calls) {
						ArrayList<String> latLons = new ArrayList<String>();
						for (CallLog call : calls) {
							latLons.add(
									call.mylat + "," + call.mylong + "," + call.contactlat + "," + call.contactlong);
						}
						return latLons.iterator();
					}
				});

		JavaRDD<String> distances = pipeInputs.pipe(SparkFiles.get(distScriptName));
		System.out.println(distances.count());
	}

	static CallLog[] readExchangeCallLog(ObjectMapper mapper, ContentExchange exchange) {
		try {
			exchange.waitForDone();
			String responseJson = exchange.getResponseContent();
			return mapper.readValue(responseJson, CallLog[].class);
		} catch (Exception e) {
			return new CallLog[0];
		}
	}

	static Tuple2<String, CallLog[]> fetchResultFromRequest(ObjectMapper mapper,
			Tuple2<String, ContentExchange> signExchange) {
		String sign = signExchange._1();
		ContentExchange exchange = signExchange._2();
		return new Tuple2(sign, readExchangeCallLog(mapper, exchange));
	}

	static Tuple2<String, ContentExchange> createRequestForSign(String sign, HttpClient client) throws Exception {
		ContentExchange exchange = new ContentExchange(true);
		exchange.setURL("http://new73s.herokuapp.com/qsos/" + sign + ".json");
		client.send(exchange);
		return new Tuple2(sign, exchange);
	}

	static ObjectMapper createMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return mapper;
	}

	static String[] loadCallSignTable() throws FileNotFoundException {
		Scanner callSignTbl = new Scanner(new File("./file/callsign_tbl_sorted"));
		ArrayList<String> callSignList = new ArrayList<String>();
		while (callSignTbl.hasNextLine()) {
			callSignList.add(callSignTbl.nextLine());
		}
		return callSignList.toArray(new String[0]);
	}

	static String lookupCountry(String callSign, String[] table) {
		Integer pos = java.util.Arrays.binarySearch(table, callSign);
		if (pos < 0) {
			pos = -pos - 1;
		}
		return table[pos].split(",")[1];
	}
}
