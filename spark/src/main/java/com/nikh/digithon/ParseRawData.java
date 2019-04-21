package com.nikh.digithon;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataTypes;
//import org.json.JSONObject;

import scala.Int;
import scala.util.parsing.json.JSONObject;

import static org.apache.spark.sql.functions.*;

public class ParseRawData {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		//Logger.getLogger("org.apache").setLevel(Level.DEBUG);
		System.setProperty("org.eclipse.jetty.LEVEL", "OFF");
		System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog");

		SparkSession spark = SparkSession.builder().appName("SQL").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();
		
		spark.conf().set("spark.shuffle.partitions", "12");
		spark.conf().set("spark.speculation","false");
		spark.conf().set("spark.debug.maxToStringFields", "100");
		Dataset<Row> modelData = spark.read().option("header", true)
									.csv("src/main/resources/digithon/LoanStats3b.csv");
		
		//modelData = modelData.first()
		//JSONObject obj = new JSONObject(obj)
		Date date = new Date();
		System.out.println(date);
		DateFormat dateFormat = new SimpleDateFormat();
		String formattedDate= dateFormat.format(date);
		
		spark.udf().register("final_dt", (Integer Loan_mid_yr, Integer iss_yr) -> 
			
			{return Loan_mid_yr+ iss_yr;
			
		}, DataTypes.IntegerType);
	
		modelData = modelData.withColumn("Today", date_format(current_date(), "y-M-d"))
					.withColumn("Loan_mid_yr", (col("term").substr(0, 3).divide(lit(12)).cast(DataTypes.IntegerType)))
					.withColumn("iss_yr", col("issue_d").substr(5, 6).cast(DataTypes.IntegerType))
					.withColumn("loan_end", concat(substring(col("issue_d"), 0, 3), callUDF("final_dt", col("Loan_mid_yr"), col("iss_yr"))).cast(DataTypes.StringType))
					.withColumn("loan_end_dt", date_add(col("Today"), 8650))
					.withColumnRenamed("inq_last_6mths", "No_OF_EMIs_PAID")
					.withColumn("Total_EMI", col("term").substr(0, 3))
					//.withColumn("Total_EMI", col("Total_EMI").cast(DataTypes.IntegerType))
					.withColumn("EMI_Defaulted", functions.randn(6).plus(3).cast(DataTypes.IntegerType))//Math.random()*6).cast(DataTypes.IntegerType))
					.withColumn("EMI_Paid", functions.randn(6).plus(8).cast(DataTypes.IntegerType))
					.withColumn("EMI_Remaining", lit(col("Total_EMI").minus(col("EMI_Paid")).cast(DataTypes.IntegerType)))
					.filter(col("loan_status").notEqual("Fully Paid"))
					.withColumn("Car_Model", functions.when(col("loan_amnt").geq(25000), "Nissan Micra").otherwise(when(col("loan_amnt").gt(15000), "Nissan Sunny").otherwise(when(col("loan_amnt").gt(8000), "Nissan NV350").otherwise("Renault Kwid"))))
					.withColumn("Age", functions.randn(60).plus(20).cast(DataTypes.IntegerType))
					.withColumn("Predict_Model", functions.when(col("Car_Model").like("Nissan Micra"), "Nissan Kicks").otherwise(when(col("Car_Model").like("Nissan Sunny"), "Nissan Terrano").otherwise(when(col("Car_Model").like("Renault Kwid"), "Renault Duster").otherwise("Nissan Leaf"))))
					.withColumn("Credit_Score", functions.when(col("grade").isin("A", "B", "C"), "Good").otherwise("Bad"))
					.withColumn("Predict_Model2", functions.when(col("Predict_Model").like("Nissan Kicks"), "Nissan Hybrid").otherwise(when(col("Predict_Model").like("Nissan Terrano"), "Nissan Patrol").otherwise(when(col("Predict_Model").like("Renault Duster"), "Nissan PathFiner").otherwise("Nissan 370Z"))))
					.withColumn("Resale_Val", col("loan_amnt").divide(lit(30)).cast(DataTypes.IntegerType));
					
		modelData.show(10);
		
		 System.out.println(modelData.toJSON());
		//System.out.println(x);
		
		//modelData.select("Credit_Score").coalesce(1).write().format("com.databricks.spark.csv")
							//.save("C:\\Users\\ndh00279\\Documents\\Digithon\\Data");
		
		//modelData.select("*").coalesce(1).write().format("json")
					//.save("C:\\Users\\ndh00279\\Documents\\Digithon\\Data");
		
		//Dataset<String> jsonDs = simpleProf.toJSON();
		//Dataset<Row> bookAsJsonDf = spark.read().json(modelData);
		//modelData.select(to_json(struct(col("*")).alias("value")));
		
		//"No_OF_EMIs_PAID", "Total_EMI", "EMI_Defaulted", "EMI_Paid", "EMI_Remaining", "Age", "loan_status"
		//"Loan_mid_yr", "iss_yr", "loan_end"

	}



}
