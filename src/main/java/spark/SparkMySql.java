
package spark;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SparkMySql {

	private static Logger logger = Logger.getLogger(SparkMySql.class);

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("SparkMySql");
		sparkConf.setMaster("local");
		JavaSparkContext sc = null;
		try {
			sc = new JavaSparkContext(sparkConf);

			SQLContext sqlContext = new SQLContext(sc);

			// 一个条件表示一个分区
			String[] predicates = new String[] { "1=1 order by id limit 400000,50000",
					"1=1 order by id limit 450000,50000", "1=1 order by id limit 500000,50000",
					"1=1 order by id limit 550000,50000", "1=1 order by id limit 600000,50000" };

			String url = "jdbc:mysql://192.168.44.128:3306/hive";
			String table = "DBS";
			Properties connectionProperties = new Properties();
			connectionProperties.setProperty("dbtable", table);// 设置表
			connectionProperties.setProperty("user", "root");// 设置用户名
			connectionProperties.setProperty("password", "Root-123456");// 设置密码

			// 读取数据
			Dataset jdbcDF = sqlContext.read().jdbc(url, table, connectionProperties).persist();
			jdbcDF.printSchema();
			System.out.println(jdbcDF.first());

//			// 写入数据
//			String url2 = "jdbc:mysql://localhost:3306/mysql";
//			Properties connectionProperties2 = new Properties();
//			connectionProperties2.setProperty("user", "root");// 设置用户名
//			connectionProperties2.setProperty("password", "root");// 设置密码
//			String table2 = "demo4";
//
//			// SaveMode.Append表示添加的模式
//
//			// SaveMode.Append:在数据源后添加；
//			// SaveMode.Overwrite:如果如果数据源已经存在记录，则覆盖；
//			// SaveMode.ErrorIfExists:如果如果数据源已经存在记录，则包异常；
//			// SaveMode.Ignore:如果如果数据源已经存在记录，则忽略；
//
//			jdbcDF.write().mode(SaveMode.Append).jdbc(url2, table2, connectionProperties2);
		} catch (Exception e) {
			logger.error("|main|exception error", e);
		} finally {
			if (sc != null) {
				sc.stop();
			}

		}

	}
}