package com.nikh.spark.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;

import scala.Tuple2;

public class KafkaStreamingSpark {
	
	public static void main(String[] args) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("StreamConnection").setMaster("local[*]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(15));
		
		Collection<String> topics = Arrays.asList("viewrecords");
		
		Map<String, Object> params = new HashMap<>();
		params.put("bootstrap.servers", "localhost:9092");
		params.put("key.deserializer", StringDeserializer.class);
		params.put("value.deserializer", StringDeserializer.class);
		params.put("group.id", "spark-group");
		params.put("auto.offset.reset", "latest");
		JavaInputDStream<ConsumerRecord<Object, Object>> streamData = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), 
						ConsumerStrategies.Subscribe(topics, params));
		//JavaDStream<Object> res = streamData.map(rec -> rec.value());
		
		JavaPairDStream<Long, Object> res = streamData.mapToPair(rec -> new Tuple2<> (rec.value(), 15L))
												.reduceByKeyAndWindow((x,y) -> x+y, Durations.minutes(1), Durations.seconds(30))
												.mapToPair(item -> item.swap())
												.transformToPair(rec -> rec.sortByKey(false));
		res.print();
		
		ssc.start();
		ssc.awaitTermination();
	}

}
