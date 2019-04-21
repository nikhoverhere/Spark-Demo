package com.nikh.spark;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;


public class SparkSQLTest {

	public static void main(String[] args) throws IOException {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
//		SparkConf conf = new SparkConf().setAppName("TestConnection").setMaster("local[*]");
//		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession.builder().appName("SQL").master("local[*]")
													.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
													.getOrCreate();
		
		spark.conf().set("spark.shuffle.partitions", "12");
		
		Dataset<Row> dataSet = spark.read().option("header", true)
											.csv("src/main/resources/exams/students.csv");
		
		
		//dataSet.show(5);
		
		Row firstRow = dataSet.first();
		
		String sub = firstRow.get(1).toString();
		String subject = firstRow.getAs("subject").toString();
		int year = Integer.parseInt(firstRow.getAs("year"));
		
		Dataset<Row> modernFilter = dataSet.filter("subject = 'Modern Art' AND year >= 2007");
		
		Dataset<Row> lambdaFilter = dataSet.filter(row -> row.getAs("subject").equals("Modern Art")
												&& Integer.parseInt(row.getAs("year")) >= 2007);
		
		Column subCol = dataSet.col("subject");
		Column yearCol = dataSet.col("year");
		
		Dataset<Row> res = dataSet.filter(subCol.equalTo("Modern Art").and(yearCol.geq(2007)));
		
		Dataset<Row> res1 = dataSet.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));

		res.show();
		
		dataSet.createOrReplaceTempView("my_students_view");
		//creates a temporary view in in_memory
		
		Dataset<Row> sqlRes = spark.sql("select max(score) from my_students_view where subject = 'French'");
		sqlRes.show();
		
		
		//In Memory
		List<Row> inMemory = new ArrayList<Row>();
		
		//inMemory.add(RowFactory.create("Warn",  "15 February 2019"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		};
		
		StructType schema = new StructType(fields);
		Dataset<Row> df = spark.createDataFrame(inMemory, schema);
		df.createOrReplaceTempView("logging_table");
		Dataset<Row> midRes = spark.sql("select level, collect_list(datetime) from logging_table group by level order by level");
		midRes.show();
		
		//spark.sql("select level, date_format(datetime, 'MM') as Month, count(1) from logging_table group by level order by level");
		//Dataset<Row> tbl = spark.sql("select level, date_format(datetime, 'MM') as Month from logging_table group by level order by level");
		//tbl.show();
		//df.show();
		
		Dataset<Row> csvSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		csvSet.createOrReplaceTempView("csv_table");
		
//		Dataset<Row> tblBig = spark.sql("select level, date_format(datetime, 'MM') as Month from csv_table");
//		tblBig.createOrReplaceTempView("csv_table");
		
		/*UDF's IN SPARK SQL */
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		
		spark.udf().register("monthNum", (String month) -> {
			
			Date inputDate = input.parse(month);
			return Integer.parseInt(output.format(inputDate));
			
		}, DataTypes.IntegerType);
		
		csvSet.createOrReplaceTempView("tmp_tbl");
		
		Dataset<Row> tmp_tbl = spark.sql("select level, date_format(datetime, 'MMMM') as month from tmp_tbl ");
		tmp_tbl.createOrReplaceTempView("tmp_tbl");
		
		Dataset<Row> tmp_out = spark.sql("select level, month, monthNum(month) from tmp_tbl");
		System.out.println("TMP TBL ELEMENTS");
		tmp_out.show();

		/////////////////////////
		
		
		Dataset<Row> result1 = spark.sql("select level, date_format(datetime, 'MMMM') as Month, " +
						" date_format(datetime, 'M')" +
						" as MonthNum from csv_table");
		result1.createOrReplaceTempView("csv_table");
		
		Dataset<Row> result2 = spark.sql("select level, Month, cast(first(MonthNum) as int) as mon," +
							" count(1) as result" + 
							" from csv_table group by level, Month order by mon, level");
		
		Dataset<Row> result3 = result2.drop("mon");
		
		result3.show(false);
		
		((Closeable) spark).close();
	}
}
