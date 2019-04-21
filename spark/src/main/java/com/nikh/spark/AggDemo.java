package com.nikh.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class AggDemo {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("SQL").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		spark.conf().set("spark.shuffle.partitions", "12");// 4 cores * 3
		Dataset<Row> students = spark.read().option("header", true)
									.csv("src/main/resources/exams/students.csv");
		
		Column score = students.col("score");
		Dataset<Row> students1 = students.groupBy("subject")
						.agg(max(col("score").cast(DataTypes.IntegerType)).alias("max_score"),
							min(col("score").cast(DataTypes.IntegerType)).alias("min_Score"));
		//You cannot cast directly call max on a grpby so call agg
		//Also if you are using agg no need for casting remove .cast
		
		students1.show();
		
		Dataset<Row> pvt = students.groupBy("subject").pivot("year")
							.agg(round(avg(col("score")), 2).alias("average"),
									round(stddev(col("score")), 2).alias("std_dev"));
		pvt.show();
		
		spark.udf().register("qualified", (String grade, String subject) -> {
			
			if(subject.equals("Biology")) {
				if(grade.startsWith("A"))
					return true;
				else
					return false;
			}
			return grade.startsWith("A+") || grade.startsWith("B") || grade.startsWith("C");
			
		}, DataTypes.BooleanType);
		//Above we cannot use equalTo as we are not delaing with cols here
		
		
		
		students = students.withColumn("dummy_col", lit(col("grade").equalTo("A+")));
		students = students.withColumn("qual", callUDF("qualified", col("grade"), col("subject")));
		students.show();
		
		students.explain();
	}
}
