package com.nikh.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class PairRDDDemo {
	
	public static void main(String[] args) {
		
	List<String> sample = new ArrayList<>();
	sample.add("WARN: Tuesday 4 September 0405");
	sample.add("ERROR: Tuesday 4 September 0408");
	sample.add("FATAL: Wednesday 5 September 1632");
	sample.add("ERROR: Friday 7 September 1854");
	sample.add("WARN: Saturday 8 September 1942");
	
	Logger.getLogger("org.apache").setLevel(Level.WARN);
	
	SparkConf conf = new SparkConf().setAppName("TestConnection").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	JavaRDD<String> srdd = sc.parallelize(sample);
	JavaPairRDD<String, Long> pairRDD = srdd.mapToPair(rawRec -> {
		String[] key = rawRec.split(":");
		String level = key[0];

		
		
		return new Tuple2<String, Long> (level, 1L);
	});
	
	JavaPairRDD<String, Long> pairRDD1 = sc.parallelize(sample)
				.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
				.reduceByKey((x,y) -> x+y);
				
				pairRDD1.foreach(x -> System.out.println(x._1 + " Has " + x._2 + " instances"));
	
	JavaPairRDD<String, Long> sum = pairRDD.reduceByKey((x, y) -> x + y);
	sum.foreach(x -> System.out.println(x._1 + " Has " + x._2 + " instances"));
	
	//GroupByKey
	sc.parallelize(sample)
		.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
		.groupByKey()
		.foreach(x -> System.out.println(x._1 + " has " + Iterables.size(x._2) + "GroupBy Instances"));
	
	JavaRDD<String> fMapUsage = sc.parallelize(sample)
									.flatMap(rec -> Arrays.asList(rec.split(" ")).iterator());
	
	JavaRDD<String> filteredWords = fMapUsage.filter(word -> word.length() > 1);
	
	
	fMapUsage.foreach(rec -> System.out.println(rec + "\n" + "Flat Map Usg"));
	
	sc.parallelize(sample).flatMap(rec -> Arrays.asList(rec.split(" ")).iterator())
		.filter(word -> word.length() > 1)
		.foreach(rec -> System.out.println(rec + "\n" + "Flat Map Usg"));
	
	sc.close();
	}
}
