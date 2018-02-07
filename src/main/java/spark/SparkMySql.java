
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

			// һ��������ʾһ������
			String[] predicates = new String[] { "1=1 order by id limit 400000,50000",
					"1=1 order by id limit 450000,50000", "1=1 order by id limit 500000,50000",
					"1=1 order by id limit 550000,50000", "1=1 order by id limit 600000,50000" };

			String url = "jdbc:mysql://192.168.44.128:3306/hive";
			String table = "DBS";
			Properties connectionProperties = new Properties();
			connectionProperties.setProperty("dbtable", table);// ���ñ�
			connectionProperties.setProperty("user", "root");// �����û���
			connectionProperties.setProperty("password", "Root-123456");// ��������

			// ��ȡ����
			Dataset jdbcDF = sqlContext.read().jdbc(url, table, connectionProperties).persist();
			jdbcDF.printSchema();
			System.out.println(jdbcDF.first());

//			// д������
//			String url2 = "jdbc:mysql://localhost:3306/mysql";
//			Properties connectionProperties2 = new Properties();
//			connectionProperties2.setProperty("user", "root");// �����û���
//			connectionProperties2.setProperty("password", "root");// ��������
//			String table2 = "demo4";
//
//			// SaveMode.Append��ʾ��ӵ�ģʽ
//
//			// SaveMode.Append:������Դ����ӣ�
//			// SaveMode.Overwrite:����������Դ�Ѿ����ڼ�¼���򸲸ǣ�
//			// SaveMode.ErrorIfExists:����������Դ�Ѿ����ڼ�¼������쳣��
//			// SaveMode.Ignore:����������Դ�Ѿ����ڼ�¼������ԣ�
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