package com.nikh.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparfDataFrame {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("SQL").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> csvSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		Dataset<Row> csvSet1 = csvSet.selectExpr("level", "date_format(datetime, 'MMMM') as month");
		Dataset<Row> csvSet2 = csvSet.select(col("level"), date_format(col("datetime"), "MMMM")
										.alias("month"), date_format(col("datetime"), "M").alias("monnum").cast(DataTypes.IntegerType));
		
		
		RelationalGroupedDataset grped = csvSet2.groupBy(col("level"), col("month"));
		
		csvSet2 = csvSet2.groupBy(col("level"), col("month"), col("monnum")).count();
		csvSet2 = csvSet2.orderBy(col("monnum"), col("level"));
		csvSet2 = csvSet2.drop("monnum");
		
		csvSet2.show(false);
		
		Dataset<Row> csvSet3 = csvSet.select(col("level"), date_format(col("datetime"), "MMMM")
				.alias("month"), date_format(col("datetime"), "M").alias("monnum").cast(DataTypes.IntegerType));

		csvSet3 = csvSet3.groupBy(col("level")).pivot("month").count();
		
		//If you want the pivoted cols in order
		Object[] mons = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August",
							"September", "October", "November", "December"};
		List<Object> columns = Arrays.asList(mons);
		
		csvSet3 = csvSet3.groupBy(col("level")).pivot("month", columns).count().na().fill(0);
		//na func: If there is a col with no value then it substitutes that value with 0
		
		csvSet3.show();
	}
}
