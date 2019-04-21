package com.nikh.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LogStreamAnalysys {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("TestConnection").setMaster("local[*]");
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(15));

		JavaReceiverInputDStream<String> inputData = ssc.socketTextStream("localhost", 8989);
		JavaDStream<Object> results = inputData.map(item -> item);
		JavaPairDStream<String, Long> resultsPair = results.mapToPair(
				msg -> new Tuple2<String, Long> (((String) msg).split(",")[0], 1L));
		//resultsPair = resultsPair.reduceByKey((x,y) -> x+y);
		resultsPair = resultsPair.reduceByKeyAndWindow(((x,y) -> x+y), Durations.minutes(2));
		resultsPair.print();
		
		ssc.start();
		ssc.awaitTermination();
	}

}
